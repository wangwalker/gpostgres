package storage

import "github.com/wangwalker/gpostgres/pkg/ast"

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

func NewTable(stmt ast.QueryStmtCreateTable) *MemoTable {
	rows := make([]Row, tableRowDefaultCount)
	return &MemoTable{
		Name:    stmt.Name,
		Columns: stmt.Columns,
		Rows:    rows,
	}
}
