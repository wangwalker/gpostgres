package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/wangwalker/gpostgres/pkg/ast"
)

const (
	tableRowDefaultCount uint8 = 100
)

type Field string

type Row []Field

type Table struct {
	Name        string           `json:"name"`
	Len         int              `json:"len"`
	Columns     []ast.Column     `json:"columns"`
	ColumnNames []ast.ColumnName `json:"column_names"`
	Rows        []Row            `json:"rows"`
}

// SaveScheme saves the scheme of a table to a file with json format when creating a table.
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

// returns the path of a table's scheme based on the table name and scheme dir.
func (t Table) schemePath() string {
	return fmt.Sprintf("%s/%s.json", config.SchemeDir, t.Name)
}

// LoadScheme loads all schemes of tables from files when starting the program.
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
