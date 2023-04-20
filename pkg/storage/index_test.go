package storage

import (
	"fmt"
	"testing"

	"github.com/wangwalker/gpostgres/pkg/ast"
)

// Tests CreateIndex
func TestCreateIndexNode(t *testing.T) {
	// GIVEN
	t1 := Table{
		Name: "testindex1",
		Columns: []ast.Column{
			{Name: "name", Kind: ast.ColumnKindText},
			{Name: "age", Kind: ast.ColumnKindInt},
		},
	}

	// WHEN
	t1.createIndex()

	// THEN
	if len(t1.index.Btrees) != 2 {
		t.Errorf("table indexes is not correct")
	}
	if t1.index.Name != "testindex1" {
		t.Errorf("table index name is not correct")
	}
	if n1 := t1.index.get("name"); n1 == nil {
		t.Errorf("table index for name column should not be nil")
	}
	if n2 := t1.index.get("age"); n2 == nil {
		t.Errorf("table index for age column should not be nil")
	}
}

func TestCreateIndexPath(t *testing.T) {
	// GIVEN
	t1 := Table{
		Name: "testindex2",
		Columns: []ast.Column{
			{Name: "name", Kind: ast.ColumnKindText},
			{Name: "age", Kind: ast.ColumnKindInt},
		},
	}

	// WHEN
	t1.createIndex()

	// THEN
	for _, c := range t1.Columns {
		path1 := t1.index.path(string(c.Name))
		path2 := fmt.Sprintf("%s/%s/%s.index", config.IndexDir, t1.Name, c.Name)
		if path1 != path2 {
			t.Errorf("table index path is not correct")
		}
	}
}

// Tests Index.Insert
func TestIndexInsertAndSearchName(t *testing.T) {
	// GIVEN
	t1 := Table{
		Name: "testindex3",
		Columns: []ast.Column{
			{Name: "name", Kind: ast.ColumnKindText},
			{Name: "age", Kind: ast.ColumnKindInt},
		},
	}
	t1.createIndex()

	// WHEN
	r := make([]Field, 0, 8)
	r = append(r, "wang", "18")
	r = append(r, "li", "32")
	r = append(r, "zhao", "28")
	r = append(r, "qian", "26")
	t1.Rows = append(t1.Rows, r)
	t1.index.insert("name", "wang", 1, 1, 0, 0)
	t1.index.insert("name", "li", 2, 1, 0, 0)
	t1.index.insert("name", "zhao", 3, 1, 0, 0)
	t1.index.insert("name", "qian", 4, 1, 0, 0)

	// THEN
	if v := t1.index.search("name", "wang"); v.Offset != 1 {
		t.Errorf("table index value is not correct")
	}
	if v := t1.index.search("name", "li"); v.Offset != 2 {
		t.Errorf("table index value is not correct")
	}
	if v := t1.index.search("name", "zhao"); v.Offset != 3 {
		t.Errorf("table index value is not correct")
	}
	if v := t1.index.search("name", "qian"); v.Offset != 4 {
		t.Errorf("table index value is not correct")
	}
}

func TestIndexInsertAndSearchAge(t *testing.T) {
	// GIVEN
	t1 := Table{
		Name: "testindex4",
		Columns: []ast.Column{
			{Name: "name", Kind: ast.ColumnKindText},
			{Name: "age", Kind: ast.ColumnKindInt},
		},
	}
	t1.createIndex()

	// WHEN
	r := make([]Field, 0, 8)
	r = append(r, "wang", "18")
	r = append(r, "li", "32")
	r = append(r, "zhao", "28")
	r = append(r, "qian", "26")
	t1.Rows = append(t1.Rows, r)
	t1.index.insert("age", "wang", 1, 1, 0, 0)
	t1.index.insert("age", "li", 2, 1, 0, 0)
	t1.index.insert("age", "zhao", 3, 1, 0, 0)
	t1.index.insert("age", "qian", 4, 1, 0, 0)

	// THEN
	if v := t1.index.search("age", "wang"); v.Offset != 1 {
		t.Errorf("table index value is not correct")
	}
	if v := t1.index.search("age", "li"); v.Offset != 2 {
		t.Errorf("table index value is not correct")
	}
	if v := t1.index.search("age", "zhao"); v.Offset != 3 {
		t.Errorf("table index value is not correct")
	}
	if v := t1.index.search("age", "qian"); v.Offset != 4 {
		t.Errorf("table index value is not correct")
	}
}
