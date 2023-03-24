package storage

import (
	"errors"

	"github.com/wangwalker/gpostgres/pkg/ast"
)

var (
	ErrTableExisted     = errors.New("table already existed")
	ErrTableNotExisted  = errors.New("table not existed")
	ErrValuesIncomplete = errors.New("inserted values isn't complete")
)

var tables = make(map[string]MemoTable)

func CreateTable(stmt *ast.QueryStmtCreateTable) error {
	tableName := stmt.Name
	if _, ok := tables[tableName]; ok {
		return ErrTableExisted
	}
	table := NewTable(*stmt)
	tables[tableName] = *table
	return nil
}

func Insert(stmt *ast.QueryStmtInsertValues) (int, error) {
	table, ok := tables[stmt.TableName]
	if !ok {
		return 0, ErrTableNotExisted
	}
	columns := len(stmt.ColumnNames)
	if stmt.ContainsAllColumns {
		columns = len(table.Columns)
	}
	// TODO: support default value for column when inserts partial columns
	// TODO: check if column value and kind matches
	rows := make([]Row, 0, len(stmt.Rows))
	for _, r := range stmt.Rows {
		row := make([]Field, 0, len(r))
		for _, v := range r {
			row = append(row, Field(v))
		}
		if len(row) != columns {
			return 0, ErrValuesIncomplete
		}
		rows = append(rows, row)
	}

	table.Rows = append(table.Rows, rows...)
	return len(rows), nil
}
