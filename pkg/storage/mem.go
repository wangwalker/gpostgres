package storage

import (
	"errors"

	"github.com/wangwalker/gpostgres/pkg/ast"
)

var (
	TableExistedError = errors.New("table already existed")
)

var tables = make(map[string]MemoTable)

func CreateTable(stmt ast.QueryStmtCreateTable) error {
	tableName := stmt.Name
	if _, ok := tables[tableName]; ok {
		return TableExistedError
	}
	table := NewTable(stmt)
	tables[tableName] = *table
	return nil
}
