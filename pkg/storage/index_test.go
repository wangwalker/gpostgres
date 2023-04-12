package storage

import (
	"testing"

	"github.com/wangwalker/gpostgres/pkg/ast"
)

// Tests CreateIndex
func TestCreateIndex(t *testing.T) {
	// GIVEN
	t1 := Table{
		Name: "testindex1",
		Columns: []ast.Column{
			{Name: "name", Kind: ast.ColumnKindText},
			{Name: "age", Kind: ast.ColumnKindInt},
		},
	}

	// WHEN
	t1.CreateIndex("name")

	// THEN
	if len(t1.Indexes) != 1 {
		t.Errorf("table indexes is not correct")
	}
	if t1.Indexes[0].Name != "name" {
		t.Errorf("table index name is not correct")
	}
}

// Tests Index.Insert
func TestIndexInsert(t *testing.T) {
	// GIVEN
	t1 := Table{
		Name: "testindex2",
		Columns: []ast.Column{
			{Name: "name", Kind: ast.ColumnKindText},
			{Name: "age", Kind: ast.ColumnKindInt},
		},
	}
	t1.CreateIndex("name")

	// WHEN
	r := make([]Field, 0, 2)
	r = append(r, "wang", "18")
	t1.Rows = append(t1.Rows, r)
	t1.Index("name").Insert("wang", 32)

	// THEN
	if t1.Indexes[0].Name != "name" {
		t.Errorf("table index key is not correct")
	}
	if v := t1.Index("name").Search("wang"); v != 32 {
		t.Errorf("table index value is not correct")
	}
}
