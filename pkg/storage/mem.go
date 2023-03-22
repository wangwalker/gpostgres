package storage

import (
	"errors"

	"github.com/wangwalker/gpostgres/pkg/ast"
)

var (
	ErrTableExisted    = errors.New("table already existed")
	ErrTableNotExisted = errors.New("table not existed")
)

var tables = make(map[string]MemoTable)

func CreateTable(stmt ast.QueryStmtCreateTable) error {
	tableName := stmt.Name
	if _, ok := tables[tableName]; ok {
		return ErrTableExisted
	}
	table := NewTable(stmt)
	tables[tableName] = *table
	return nil
}
