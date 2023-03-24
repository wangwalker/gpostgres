package storage

import (
	"errors"

	"github.com/wangwalker/gpostgres/pkg/ast"
)

var (
	ErrTableExisted     = errors.New("table already existed")
	ErrTableNotExisted  = errors.New("table not existed")
	ErrValuesImcomplete = errors.New("inserted values isn't complete")
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
	row := make([]ast.ColumnValue, 0, len(stmt.ColumnValues))
	if stmt.ContainsAllColumns {
		if len(table.Columns) != len(stmt.ColumnValues) {
			return 0, ErrValuesImcomplete
		}
		// TODO: check if column value and kind matches
		for _, v := range stmt.ColumnValues {
			row = append(row, ast.ColumnValue(v))
		}
	} else {
		// TODO: Support default value for column for partial columns insertion
		for _, v := range stmt.ColumnValues {
			row = append(row, ast.ColumnValue(v))
		}
	}
	table.Rows = append(table.Rows, row)
	return 1, nil
}
