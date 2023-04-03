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
	Rows        []Row            `json:"rows"`
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

// loadScheme loads a scheme of a table from a file with json format.
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

// GenerateAvroCodec returns the avro codec based on table scheme. As goavro says,
// codec ought to be cached to avoid the overhead of parsing the schema.
func (t Table) generateAvroCodec() (*goavro.Codec, error) {
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

// SaveRows saves rows of a table to a file with Avro binary format when inserting a row.
// For many rows, we should call this serially.
func (t Table) saveRows(rows []Row) (int, error) {
	_, err := os.Stat(config.DataDir)
	if os.IsNotExist(err) {
		os.Mkdir(config.DataDir, 0755)
	}
	codec, err := t.generateAvroCodec()
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
	w := bufio.NewWriter(f)
	for _, r := range rows {
		record := make(map[string]interface{})
		for i, c := range t.Columns {
			name := string(c.Name)
			if c.Kind == ast.ColumnKindInt {
				ii, _ := strconv.Atoi(string(r[i]))
				record[name] = ii
			} else {
				record[name] = string(r[i])
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

// LoadRows loads binary rows data of all tables from local binary data to native Row when
// program launches. Specifically, it should be called after finishing loading schemes.
func loadRows() {
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
	codec, err := t.generateAvroCodec()
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

// Show scheme of a table like below
/**
 Column  |            Type             |
event_id | integer                     |
title    | character varying(255)      |
venue_id | integer                     |
*/
func (t Table) String() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("| %-10s | %-20s|\n", "Column", "Type"))
	sb.WriteString(fmt.Sprintf("|-%-10s-+-%-20s|\n", strings.Repeat("-", 10), strings.Repeat("-", 20)))
	for _, c := range t.Columns {
		sb.WriteString(fmt.Sprintf("| %-10s | %-20s|\n", c.Name, c.Kind))
	}
	return sb.String()
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

func ShowTableSchemes(t string) {
	if t == "" {
		names := make([]string, 0, len(tables))
		for table := range tables {
			names = append(names, table)
		}
		fmt.Printf("List of relations\n%s", strings.Join(names, "\n"))
		return
	}
	table, ok := tables[t]
	if !ok {
		fmt.Printf("Don't find any relations named %s", t)
	}
	fmt.Println(table.String())
}

func ShowRows(rows []Row, stmt *ast.QueryStmtSelectValues) {
	if len(rows) == 0 {
		return
	}
	columns := stmt.ColumnNames
	if len(columns) == 0 {
		table := tables[stmt.TableName]
		columns = append(columns, table.ColumnNames...)
	}
	var sb, sp strings.Builder
	sp1, sp2, sp3, sp4, sp5 := " | ", "-+-", "| ", "|-", "--"
	widthFormats := columnFormats(rows, columns)
	sb.WriteString(sp3)
	sp.WriteString(sp4)
	for i, c := range columns {
		f := widthFormats[i]
		sb.WriteString(fmt.Sprintf(f.format, c, sp1))
		if i < len(columns)-1 {
			sp.WriteString(fmt.Sprintf(f.format, strings.Repeat("-", f.width), sp2))
		} else {
			sp.WriteString(fmt.Sprintf(f.format, strings.Repeat("-", f.width), sp5))
		}
	}
	sp.WriteByte('\n')
	sb.WriteByte('\n')
	sb.WriteString(sp.String())
	for ri, r := range rows {
		sb.WriteString(sp3)
		for i, f := range r {
			sb.WriteString(fmt.Sprintf(widthFormats[i].format, f, sp1))
		}
		if ri < len(rows)-1 {
			sb.WriteByte('\n')
		}
	}
	fmt.Println(sb.String())
}

type widthFormat struct {
	width  int
	format string
}

func columnFormats(rows []Row, columnNames []ast.ColumnName) []widthFormat {
	widths := make([]int, 0, len(columnNames))
	for _, n := range columnNames {
		widths = append(widths, len(n))
	}
	for _, r := range rows {
		for c, f := range r {
			if len(f) > widths[c] {
				widths[c] = len(f)
			}
		}
	}
	formats := make([]widthFormat, 0, len(widths))
	for _, w := range widths {
		formats = append(formats, formatForWidth(w))
	}
	return formats
}

func formatForWidth(w int) widthFormat {
	if w <= 5 {
		return widthFormat{5, "%-5s%s"}
	} else if w <= 10 {
		return widthFormat{10, "%-10s%s"}
	} else if w <= 20 {
		return widthFormat{20, "%-20s%s"}
	} else {
		return widthFormat{50, "%-50s%s"}
	}
}
