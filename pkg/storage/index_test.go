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
	if bt := t1.index.getBtree("name"); bt == nil {
		t.Errorf("table btree index for name column should not be nil")
	}
	if bt := t1.index.getBtree("age"); bt == nil {
		t.Errorf("table btree index for age column should not be nil")
	}
	if lsmt := t1.index.getLsmTree("name"); lsmt == nil {
		t.Errorf("table lsmtree index for name column should not be nil")
	}
	if lsmt := t1.index.getLsmTree("age"); lsmt == nil {
		t.Errorf("table lsmtree index for age column should not be nil")
	}
}

func TestCreateBtreeIndexDirAndPath(t *testing.T) {
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
		// for btree index
		p1 := path(indexTypeBtree, t1.Name, string(c.Name))
		p2 := fmt.Sprintf("%s/%s/%s/%s.index", config.IndexDir, btreeDir, t1.Name, c.Name)
		if p1 != p2 {
			t.Errorf("table btree index file path is not correct")
		}
		// for lsmtree index
		d1 := dir(indexTypeLsmTree, t1.Name)
		d2 := fmt.Sprintf("%s/%s/%s", config.IndexDir, lsmtDir, t1.Name)
		if d1 != d2 {
			t.Errorf("table lsmtree index file dir is not correct")
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

func TestLoadIndex(t *testing.T) {
	// GIVEN
	t1 := Table{
		Name: "testindex5",
		Columns: []ast.Column{
			{Name: "name", Kind: ast.ColumnKindText},
			{Name: "age", Kind: ast.ColumnKindInt},
		},
	}
	t1.createIndex()
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
	t1.index.insert("age", "wang", 1, 1, 0, 0)
	t1.index.insert("age", "li", 2, 1, 0, 0)
	t1.index.insert("age", "zhao", 3, 1, 0, 0)
	t1.index.insert("age", "qian", 4, 1, 0, 0)

	// WHEN
	// Normally, we should load table from scheme file, but here we just
	// create a new table that has same name and columns with t1, and load
	// index from file.
	t2 := Table{
		Name: "testindex5",
		Columns: []ast.Column{
			{Name: "name", Kind: ast.ColumnKindText},
			{Name: "age", Kind: ast.ColumnKindInt},
		},
	}
	t2.loadIndex()

	// THEN
	if t2.Name != t1.Name {
		t.Errorf("table name is not correct")
	}
	if len(t2.Columns) != len(t1.Columns) {
		t.Errorf("table columns is not correct")
	}
	if len(t2.index.Btrees) != len(t1.index.Btrees) {
		t.Errorf("table btrees is not correct")
	}
	if len(t2.index.LsmTrees) != len(t1.index.LsmTrees) {
		t.Errorf("table lsmtrees is not correct")
	}
}
