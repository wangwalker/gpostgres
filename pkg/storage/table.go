package storage

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/linkedin/goavro/v2"
	"github.com/wangwalker/gpostgres/pkg/ast"
)

const (
	tableRowDefaultCount uint8 = 100
	rowSeparator         byte  = '\n'
)

var (
	errConvertIntFailed    = errors.New("failed to convert binary to int")
	errConvertTextFailed   = errors.New("failed to convert binary to text")
	errConvertRecordFailed = errors.New("failed to convert binary to record")
)

type Field string

// Purify removes the quotes of a string when inserting new rows.
func (f Field) purify() Field {
	pure := strings.ReplaceAll(string(f), "'", "")
	pure = strings.ReplaceAll(pure, "\"", "")
	return Field(pure)
}

type Row []Field

type Table struct {
	Name        string           `json:"name"`
	Len         int              `json:"len"`
	Columns     []ast.Column     `json:"columns"`
	ColumnNames []ast.ColumnName `json:"column_names"`
	Rows        []Row
	index       *Index
	avroCodec   *goavro.Codec
}

// Convert converts a row for table to  type map[string]interface{}
// with column name as key and column value as value, which is used
// for encoding to avro binary in save(row) method.
func (t Table) convert(r Row) map[string]interface{} {
	record := make(map[string]interface{})
	for i, c := range t.Columns {
		name := string(c.Name)
		var v interface{}
		if c.Kind == ast.ColumnKindInt {
			ii, _ := strconv.Atoi(string(r[i]))
			v = ii
		} else {
			v = string(r[i])
		}
		record[name] = v
	}
	return record
}

// Get gets the stringed value of a column with the column name, which is
// used for updating index for the column.
// The parameter r is the result of call convert(row) method.
func get(r map[string]interface{}, name string) string {
	v, ok := r[name]
	if !ok {
		panic(fmt.Sprintf("column %s not found", name))
	}
	sv, ok := v.(string)
	if ok {
		return sv
	}
	return strconv.Itoa(v.(int))

}

// SaveScheme saves table scheme to file with json format when creating one.
func (t Table) saveScheme() {
	_, err := os.Stat(config.SchemeDir)
	if os.IsNotExist(err) {
		os.Mkdir(config.SchemeDir, 0755)
	}
	path := t.schemePath()
	bytes, err := json.Marshal(t)
	if err != nil {
		fmt.Printf("Failed to marshal table %s scheme to json: %s", t.Name, err)
		return
	}
	err = os.WriteFile(path, bytes, 0644)
	if err != nil {
		fmt.Printf("Failed to write table %s scheme to file: %s", t.Name, err)
	}
}

// returns the path of a table's local scheme file.
func (t Table) schemePath() string {
	return fmt.Sprintf("%s/%s.json", config.SchemeDir, t.Name)
}

// returns the path of a table's local data file.
func (t Table) dataPath() string {
	return fmt.Sprintf("%s/%s.avro", config.DataDir, t.Name)
}

// LoadScheme loads all schemes of tables from files when starting the program.
// It should be called in init function of storage package.
func loadSchemes() {
	_, err := os.Stat(config.SchemeDir)
	if err != nil && os.IsNotExist(err) {
		// create scheme dir when first run program
		_ = os.MkdirAll(config.SchemeDir, 0755)
	}
	files, err := os.ReadDir(config.SchemeDir)
	if err != nil {
		fmt.Printf("Failed to read data directory %s: %s", config.SchemeDir, err)
		return
	}
	for _, f := range files {
		if !f.IsDir() && strings.HasSuffix(f.Name(), ".json") {
			loadScheme(f.Name())
		}
	}
}

// LoadScheme loads table scheme from local json file when starting the program.
func loadScheme(name string) {
	path := fmt.Sprintf("%s/%s", config.SchemeDir, name)
	file, err := os.Open(path)
	if err != nil {
		fmt.Printf("Failed to open file %s: %s", path, err)
		return
	}

	var table Table
	err = json.NewDecoder(file).Decode(&table)
	if err != nil {
		fmt.Printf("Failed to decode json file %s: %s", path, err)
		return
	}
	table.loadIndex()
	tables[table.Name] = table
}

// ComposeAvroCodec composes avro codec based on table scheme. As goavro says,
// codec ought to be cached to avoid the overhead of parsing the schema. so we
// save the codec in table struct after finishing composing.
func (t Table) composeAvroCodec() (*goavro.Codec, error) {
	if t.avroCodec != nil {
		return t.avroCodec, nil
	}
	var fields strings.Builder
	for i, c := range t.Columns {
		if c.Kind == ast.ColumnKindInt {
			s := fmt.Sprintf(`{"name": "%s", "type": "int", "default": 0}`, c.Name)
			fields.WriteString(s)
		} else {
			s := fmt.Sprintf(`{"name": "%s", "type": "string", "default": ""}`, c.Name)
			fields.WriteString(s)
		}
		if i < len(t.Columns)-1 {
			fields.WriteString(",")
		}
	}
	var err error
	t.avroCodec, err = goavro.NewCodec(`{
		"type": "record",
		"name": "User",
		"fields" : [` + fields.String() + "]" + `
	}`)
	if err != nil {
		return nil, err
	}
	return t.avroCodec, nil
}

// Save saves rows to local Avro binary file when inserting rows.
// For many rows, we should call this serially.
func (t Table) save(rows []Row) (int, error) {
	_, err := os.Stat(config.DataDir)
	if os.IsNotExist(err) {
		os.Mkdir(config.DataDir, 0755)
	}
	codec, err := t.composeAvroCodec()
	if err != nil {
		fmt.Println(err)
		return 0, err
	}
	f, err := os.OpenFile(t.dataPath(), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	fs, _ := f.Stat()
	size := fs.Size()
	if err != nil {
		fmt.Println(err)
		return 0, err
	}
	defer f.Close()

	// accumulated size of rows inserted in this call
	var size1, offset uint16
	// write rows into file with Avro binary format
	w := bufio.NewWriter(f)
	for _, r := range rows {
		record := t.convert(r)
		bytes, err := codec.BinaryFromNative(nil, record)
		bytes = append(bytes, rowSeparator)
		if err != nil {
			fmt.Println(err)
		}
		_, err = w.Write(bytes)
		if err != nil {
			fmt.Printf("Write %v to file failed.\n", bytes)
		}
		// update index for all columns
		// Note: we don't use page and block now, so we set them to 0
		// TODO: organize row binary data into pages and blocks later
		l := uint16(len(bytes))
		size1 += l
		for _, c := range t.Columns {
			if idx := t.index; idx != nil {
				var p, b uint16
				c := string(c.Name)
				n := get(record, c)
				idx.insert(c, n, offset, l, p, b)
			}
		}
		offset = uint16(size) + size1
	}
	w.Flush()
	return len(rows), nil
}

// Load loads rows data for all tables from local binary data to native row when
// program launches.
// Note, it should be called after finishing loading schemes.
func load() {
	if len(tables) <= 0 {
		return
	}
	newTables := make(map[string]Table)
	for _, t := range tables {
		newT := t
		rows, err := t.loadRows()
		if err == nil && len(rows) > 0 {
			newT.Rows = append(newT.Rows, rows...)
		}
		newTables[t.Name] = newT
	}
	tables = newTables
}

// LoadRows loads binary rows of a table from local Avro format to Rows.
// It is the reversed process of SaveRows.
func (t *Table) loadRows() ([]Row, error) {
	_, err := os.Stat(config.DataDir)
	if os.IsNotExist(err) {
		os.Mkdir(config.DataDir, 0755)
	}
	_, err = os.Stat(t.dataPath())
	if os.IsNotExist(err) {
		return nil, err
	}
	f, err := os.OpenFile(t.dataPath(), os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	r := bufio.NewReader(f)
	rows := make([]Row, 0, len(t.Columns))
	for {
		line, err := r.ReadBytes(rowSeparator)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		if len(line) == 1 && line[0] == rowSeparator {
			continue
		}
		r, err := t.decodeRow(line)
		if err != nil {
			return nil, err
		}
		rows = append(rows, r)

	}
	return rows, nil
}

func (t *Table) setColumnNames() {
	cn := make([]ast.ColumnName, 0, len(t.Columns))
	for _, c := range t.Columns {
		cn = append(cn, c.Name)
	}
	t.ColumnNames = cn
}

func NewTable(stmt ast.QueryStmtCreateTable) *Table {
	rows := make([]Row, 0, tableRowDefaultCount)
	t := &Table{
		Name:    stmt.Name,
		Columns: stmt.Columns,
		Rows:    rows,
	}
	t.createIndex()
	return t
}

// DecodeRow decodes a row from binary data.
func (t Table) decodeRow(b []byte) (Row, error) {
	codec, err := t.composeAvroCodec()
	if err != nil {
		return nil, err
	}
	native, _, err := codec.NativeFromBinary(b)
	if err != nil || native == nil {
		return nil, err
	}
	record, ok := native.(map[string]interface{})
	if !ok {
		return nil, errConvertRecordFailed
	}
	row := make(Row, 0, len(t.Columns))
	for _, c := range t.Columns {
		v := record[string(c.Name)]
		if c.Kind == ast.ColumnKindInt {
			iv, ok := v.(int32)
			if !ok {
				return nil, errConvertIntFailed
			}
			row = append(row, Field(strconv.Itoa(int(iv))))
		} else {
			sv, ok := v.(string)
			if !ok {
				return nil, errConvertTextFailed
			}
			row = append(row, Field(sv).purify())
		}
	}
	return row, nil
}
