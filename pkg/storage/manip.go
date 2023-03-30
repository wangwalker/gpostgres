package storage

import (
	"errors"

	"github.com/wangwalker/gpostgres/pkg/ast"
	"golang.org/x/exp/slices"
)

var (
	ErrTableExisted          = errors.New("table already existed")
	ErrTableNotExisted       = errors.New("table not existed")
	ErrValuesIncomplete      = errors.New("inserted values isn't complete")
	ErrColumnNamesNotMatched = errors.New("table column names aren't matched")
)

func CreateTable(stmt *ast.QueryStmtCreateTable) error {
	tableName := stmt.Name
	if _, ok := tables[tableName]; ok {
		return ErrTableExisted
	}
	table := NewTable(*stmt)
	table.setColumnNames()
	table.saveScheme()
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
		if !slices.Contains(table.ColumnNames, sc) {
			return nil, ErrColumnNamesNotMatched
		}
	}
	// check if the column from where clause has been defined
	if !stmt.Where.IsEmpty() && !slices.Contains(table.ColumnNames, stmt.Where.Column) {
		return nil, ErrColumnNamesNotMatched
	}

	filtered := table.Rows
	if !stmt.Where.IsEmpty() {
		filtered, _ = table.filter(stmt.Where)
	}
	rows := make([]Row, 0)
	if stmt.ContainsAllColumns {
		return filtered, nil
	}

	selectedIndexes := indexesOf(stmt.ColumnNames, table.ColumnNames)
	for _, r := range filtered {
		row := make([]Field, 0, len(stmt.ColumnNames))
		for _, si := range selectedIndexes {
			for i, f := range r {
				if si == i {
					f := f
					row = append(row, f)
				}
			}
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func Update(stmt *ast.QueryStmtUpdateValues) (int, error) {
	table, ok := tables[stmt.TableName]
	if !ok {
		return 0, ErrTableNotExisted
	}
	// check if the selected columns have been defined
	for _, c := range stmt.Values {
		if !slices.Contains(table.ColumnNames, c.Name) {
			return 0, ErrColumnNamesNotMatched
		}
	}
	// check if the column from where clause has been defined
	if !stmt.Where.IsEmpty() && !slices.Contains(table.ColumnNames, stmt.Where.Column) {
		return 0, ErrColumnNamesNotMatched
	}

	filtered, _ := table.filter(stmt.Where)
	for _, r := range filtered {
		r.update(stmt.Values, table)
	}
	return len(filtered), nil
}

// Returns the indexes of sub slice from a slice. For expample:
// names := []string{"a", "b", "c"}
// subnames := []string{"b", "c"}
// indexesOf(subnames, names) = []int{1, 2}
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

// Returns all the rows and indexes meeting where clause for one table.
func (mt Table) filter(where ast.WhereClause) ([]Row, []int) {
	filtered := make([]Row, 0, mt.Len)
	indexes := make([]int, 0, mt.Len)
	columnIndex := slices.Index(mt.ColumnNames, where.Column)
OUTER:
	for _, cn := range mt.ColumnNames {
		for i, r := range mt.Rows {
			if cn != where.Column {
				continue OUTER
			}
			if r.matched(where, columnIndex) {
				filtered = append(filtered, r)
				indexes = append(indexes, i)
			}
		}
	}
	return filtered, indexes
}

// Tests if one row if matched with where clause.
// Note: the row is just slice of Field, so using index indicates which field to test.
func (r Row) matched(where ast.WhereClause, index int) bool {
	switch where.Cmp {
	case ast.CmpKindEq:
		return r[index] == Field(where.Value)
	case ast.CmpKindNotEq:
		return r[index] != Field(where.Value)
	case ast.CmpKindGt:
		return r[index] > Field(where.Value)
	case ast.CmpKindGte:
		return r[index] >= Field(where.Value)
	case ast.CmpKindLt:
		return r[index] < Field(where.Value)
	case ast.CmpKindLte:
		return r[index] <= Field(where.Value)
	}
	return false
}

func (r Row) update(newValues []ast.ColumnUpdatedValue, table Table) {
	sub := make([]ast.ColumnName, 0, len(newValues))
	for _, v := range newValues {
		sub = append(sub, v.Name)
	}
	indexes := indexesOf(sub, table.ColumnNames)
	for _, nv := range newValues {
		for i := range r {
			if slices.Contains(indexes, i) {
				r[i] = Field(nv.Value)
			}
		}
	}
}
