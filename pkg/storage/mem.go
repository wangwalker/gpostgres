package storage

import (
	"errors"

	"github.com/wangwalker/gpostgres/pkg/lexer"
)

var (
	TableExistedError = errors.New("table already existed")
)

var tables = make(map[string]Table)

func CreateTable(stmt lexer.QueryStmtCreateTable) error {
	tableName := stmt.Name.Value
	if _, ok := tables[tableName]; ok {
		return TableExistedError
	}
	table := NewTable(tableName)
	tables[tableName] = *table
	return nil
}
