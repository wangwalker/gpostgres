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
	errConvertIntFailed  = errors.New("failed to convert binary to int")
	errConvertTextFailed = errors.New("failed to convert binary to text")
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

// saveScheme saves the scheme of a table to file with json format when created.
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
	files, err := os.ReadDir(config.SchemeDir)
	if err != nil {
		fmt.Printf("Failed to read data directory %s: %s", config.DataDir, err)
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
			fields.WriteString(fmt.Sprintf(`{"name": "%s", "type": "int", "default": 0}`, c.Name))
		} else {
			fields.WriteString(fmt.Sprintf(`{"name": "%s", "type": "string", "default": ""}`, c.Name))
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
	if err != nil {
		fmt.Println(err)
		return 0, err
	}
	defer f.Close()
	// write rows into file with Avro binary format
	w := bufio.NewWriter(f)
	for ii, r := range rows {
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
			// insert indexes with row index now
			// TODO: insert indexes with more info with file format later
			if idx := t.index; idx != nil {
				v := uint16(len(t.Rows) + ii)
				idx.insert(name, string(r[i]), v)
			}
		}
		bytes, err := codec.BinaryFromNative(nil, record)
		bytes = append(bytes, rowSeparator)
		if err != nil {
			fmt.Println(err)
		}
		_, err = w.Write(bytes)
		if err != nil {
			fmt.Printf("Write %v to file failed.\n", bytes)
		}
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
	codec, err := t.composeAvroCodec()
	if err != nil {
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
		native, _, err := codec.NativeFromBinary(line)
		if err != nil || native == nil {
			return nil, err
		}
		record, ok := native.(map[string]interface{})
		if !ok {
			continue
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
		rows = append(rows, row)

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
	return &Table{
		Name:    stmt.Name,
		Columns: stmt.Columns,
		Rows:    rows,
	}
}
