package storage

import (
	"errors"
	"fmt"

	"github.com/wangwalker/gpostgres/pkg/ast"
	"golang.org/x/exp/slices"
)

var (
	ErrTableExisted          = errors.New("table already existed")
	ErrTableNotExisted       = errors.New("table not existed")
	ErrValuesIncomplete      = errors.New("inserted values isn't complete")
	ErrColumnNamesNotMatched = errors.New("table column names aren't matched")
)

var tables = make(map[string]MemoTable)

// for testing from REPL
func init() {
	table := MemoTable{
		Name: "tusers",
		Columns: []ast.Column{
			{Name: "name", Kind: ast.ColumnKindText},
			{Name: "age", Kind: ast.ColumnKindInt},
		},
		Rows: []Row{
			[]Field{"'w'", "12"},
			[]Field{"'c'", "13"},
			[]Field{"'d'", "15"},
		},
		Len: 3,
	}
	tables[table.Name] = table
}

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
	if len(stmt.Rows) < 1 {
		return 0, ErrValuesIncomplete
	}
	table, ok := tables[stmt.TableName]
	if !ok {
		return 0, ErrTableNotExisted
	}
	if len(stmt.ColumnNames) > 0 {
		for i, c := range table.Columns {
			if c.Name != stmt.ColumnNames[i] {
				return 0, ErrColumnNamesNotMatched
			}
		}
	}
	columns := len(stmt.ColumnNames)
	if stmt.ContainsAllColumns {
		columns = len(table.Columns)
	}
	// TODO: support default value for column when inserts partial columns
	rows := make([]Row, 0, len(stmt.Rows))
	fmt.Printf("before new inserted rows: %v\n", stmt.Rows)
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
	fmt.Printf("after new inserted rows: %v\n", rows)
	table.Rows = append(table.Rows, rows...)
	table.Len = len(table.Rows)
	tables[table.Name] = table
	return len(rows), nil
}

func Select(stmt *ast.QueryStmtSelectValues) ([]Row, error) {
	table, ok := tables[stmt.TableName]
	if !ok {
		return nil, ErrTableNotExisted
	}
	// check if the selected columns have been defined
	for _, sc := range stmt.ColumnNames {
		if !slices.Contains(table.columnNames(), sc) {
			return nil, ErrColumnNamesNotMatched
		}
	}

	filtered := table.Rows
	if !stmt.Where.IsEmpty() {
		filtered = table.filter(stmt.Where)
	}
	rows := make([]Row, 0)
	if stmt.ContainsAllColumns || len(stmt.ColumnNames) == len(table.Columns) {
		return filtered, nil
	}

	selectedIndexes := indexesOf(stmt.ColumnNames, table.columnNames())
	for _, r := range filtered {
		row := make([]Field, 0, len(stmt.ColumnNames))
		for i, f := range r {
			if slices.Contains(selectedIndexes, i) {
				f := f
				row = append(row, f)
			}
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func indexesOf(sub, columns []ast.ColumnName) []int {
	selectedIndexes := make([]int, 0, len(sub))
	for _, n := range sub {
		for i, c := range columns {
			if c == n {
				selectedIndexes = append(selectedIndexes, i)
			}
		}
	}
	return selectedIndexes
}

func (mt MemoTable) filter(with ast.WhereClause) []Row {
	filtered := make([]Row, 0, mt.Len)
	columnIndex := slices.Index(mt.columnNames(), with.Column)
OUTER:
	for _, cn := range mt.columnNames() {
		for _, r := range mt.Rows {
			if cn != with.Column {
				continue OUTER
			}
			switch with.Cmp {
			case ast.CmpKindEq:
				if r[columnIndex] == Field(with.Value) {
					filtered = append(filtered, r)
				}
			case ast.CmpKindNotEq:
				if r[columnIndex] != Field(with.Value) {
					filtered = append(filtered, r)
				}
			case ast.CmpKindGt:
				if r[columnIndex] > Field(with.Value) {
					filtered = append(filtered, r)
				}
			case ast.CmpKindGte:
				if r[columnIndex] >= Field(with.Value) {
					filtered = append(filtered, r)
				}
			case ast.CmpKindLt:
				if r[columnIndex] < Field(with.Value) {
					filtered = append(filtered, r)
				}
			case ast.CmpKindLte:
				if r[columnIndex] <= Field(with.Value) {
					filtered = append(filtered, r)
				}
			}
		}
	}
	return filtered
}
