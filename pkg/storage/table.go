package storage

import (
	"fmt"
	"strings"

	"github.com/wangwalker/gpostgres/pkg/ast"
)

const (
	tableRowDefaultCount uint8 = 100
)

type Field string

type Row []Field

type MemoTable struct {
	Name        string
	Len         int
	Columns     []ast.Column
	ColumnNames []ast.ColumnName
	Rows        []Row
}

// Show scheme of a table like below
/**
 Column  |            Type             |
event_id | integer                     |
title    | character varying(255)      |
venue_id | integer                     |
*/
func (mt MemoTable) String() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("%-10s | %-20s|\n", "Column", "Type"))
	sb.WriteString(fmt.Sprintf("%-10s + %-20s|\n", strings.Repeat("-", 10), strings.Repeat("-", 20)))
	for _, c := range mt.Columns {
		sb.WriteString(fmt.Sprintf("%-10s | %-20s|\n", c.Name, c.Kind))
	}
	return sb.String()
}

func (mt *MemoTable) SetColumnNames() {
	cn := make([]ast.ColumnName, 0, len(mt.Columns))
	for _, c := range mt.Columns {
		cn = append(cn, c.Name)
	}
	mt.ColumnNames = cn
}

func NewTable(stmt ast.QueryStmtCreateTable) *MemoTable {
	rows := make([]Row, 0, tableRowDefaultCount)
	return &MemoTable{
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
	sp1, sp2 := " | ", " + "
	widthFormats := columnFormats(rows, columns)
	for i, c := range columns {
		f := widthFormats[i]
		sb.WriteString(fmt.Sprintf(f.format, c, sp1))
		sp.WriteString(fmt.Sprintf(f.format, strings.Repeat("-", f.width), sp2))
	}
	sp.WriteByte('\n')
	sb.WriteByte('\n')
	sb.WriteString(sp.String())
	for _, r := range rows {
		for i, f := range r {
			sb.WriteString(fmt.Sprintf(widthFormats[i].format, f, sp1))
		}
		sb.WriteByte('\n')
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
		return widthFormat{5, " %-5s%s"}
	} else if w <= 10 {
		return widthFormat{10, " %-10s%s"}
	} else if w <= 20 {
		return widthFormat{20, " %-20s%s"}
	} else {
		return widthFormat{50, " %-50s%s"}
	}
}
