package storage

import (
	"fmt"
	"strings"

	"github.com/wangwalker/gpostgres/pkg/ast"
)

const (
	tableRowDefaultCount uint8 = 100
)

type Row []string

type MemoTable struct {
	Name    string
	Len     uint8
	Columns []ast.Column
	Rows    []Row
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

func NewTable(stmt ast.QueryStmtCreateTable) *MemoTable {
	rows := make([]Row, tableRowDefaultCount)
	return &MemoTable{
		Name:    stmt.Name,
		Columns: stmt.Columns,
		Rows:    rows,
	}
}

func ShowTableSchemes(t string) (string, error) {
	if t == "" {
		names := make([]string, 0, len(tables))
		for table := range tables {
			names = append(names, table)
		}
		return fmt.Sprintf("List of relations\n%s", strings.Join(names, "\n")), nil
	}
	table, ok := tables[t]
	if !ok {
		return fmt.Sprintf("Don't find any relations named %s", t), ErrTableNotExisted
	}

	return table.String(), nil
}
