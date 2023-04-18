package storage

import (
	"os"
	"strings"
	"testing"

	"github.com/wangwalker/gpostgres/pkg/ast"
)

func init() {
	c := &Config{
		Database:  "testdb",
		SchemeDir: "./tempstorage/scheme",
		DataDir:   "./tempstorage/data",
		IndexDir:  "./tempstorage/index",
		Mode:      "debug",
	}
	config = *c

	// remove dirs and data if exist
	os.RemoveAll(config.SchemeDir)
	os.RemoveAll(config.DataDir)
	os.RemoveAll(config.IndexDir)

	// create new dirs
	os.MkdirAll(config.SchemeDir, 0755)
	os.MkdirAll(config.DataDir, 0755)
	os.Mkdir(config.IndexDir, 0755)
}

// TestSaveScheme tests the function saveScheme.
func TestSaveScheme(t *testing.T) {
	// GIVEN
	table := Table{
		Name: "testuser1",
		Columns: []ast.Column{
			{Name: "name", Kind: ast.ColumnKindText},
			{Name: "age", Kind: ast.ColumnKindInt},
		},
	}

	// WHEN
	table.saveScheme()

	// THEN, test the scheme file is created.
	path := table.schemePath()
	if !strings.HasSuffix(path, "testuser1.json") {
		t.Errorf("scheme file path is not correct")
	}

	// THEN, test the scheme file is correct.
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		t.Errorf("scheme file is not created")
	}
}

// TestLoadSchemes tests the function loadSchemes.
func TestLoadSchemes(t *testing.T) {
	// GIVEN
	t1 := Table{
		Name: "testuser2",
		Columns: []ast.Column{
			{Name: "name", Kind: ast.ColumnKindText},
			{Name: "age", Kind: ast.ColumnKindInt},
		},
	}
	t1.saveScheme()

	// WHEN
	loadSchemes()

	// THEN
	t2, ok := tables["testuser2"]
	if !ok {
		t.Errorf("table testuser2 is not loaded")
	}
	if t2.Name != "testuser2" {
		t.Errorf("table name is not correct")
	}
	if len(t2.Columns) != 2 {
		t.Errorf("table columns is not correct")
	}
	if t2.Columns[0].Name != "name" {
		t.Errorf("table column name is not correct")
	}
	if t2.Columns[0].Kind != ast.ColumnKindText {
		t.Errorf("table column kind is not correct")
	}
	if t2.Columns[1].Name != "age" {
		t.Errorf("table column name is not correct")
	}
	if t2.Columns[1].Kind != ast.ColumnKindInt {
		t.Errorf("table column kind is not correct")
	}
}

// TestGenerateAvroCodec tests the function generateAvroSchema.
func TestComposeAvroCodec(t *testing.T) {
	// GIVEN
	table := Table{
		Name: "testuser3",
		Columns: []ast.Column{
			{Name: "name", Kind: ast.ColumnKindText},
			{Name: "age", Kind: ast.ColumnKindInt},
		},
	}

	// WHEN
	codec, err := table.composeAvroCodec()

	// THEN
	if err != nil {
		t.Errorf("failed to generate avro codec: %s", err)
	}
	if codec == nil {
		t.Errorf("avro codec is nil")
	}
}

// TestSaveRows tests the function saveRows.
func TestSaveRows(t *testing.T) {
	// GIVEN
	t1 := Table{
		Name: "testuser4",
		Columns: []ast.Column{
			{Name: "name", Kind: ast.ColumnKindText},
			{Name: "age", Kind: ast.ColumnKindInt},
		},
	}
	t1.saveScheme()

	// WHEN
	rows := make([]Row, 0, 2)
	r1 := Row{Field("wang"), Field("18")}
	r2 := Row{Field("li"), Field("20")}
	rows = append(rows, r1, r2)
	n, err := t1.save(rows)

	// THEN
	if err != nil {
		t.Errorf("failed to save rows: %s", err)
	}
	if n != len(rows) {
		t.Errorf("saved rows number is not correct")
	}
}

// TestLoadRows tests the function loadRows.
func TestLoadRows(t *testing.T) {
	// GIVEN
	t1 := Table{
		Name: "testuser5",
		Columns: []ast.Column{
			{Name: "name", Kind: ast.ColumnKindText},
			{Name: "age", Kind: ast.ColumnKindInt},
		},
	}
	t1.saveScheme()

	// WHEN
	rows := make([]Row, 0, 2)
	r1 := Row{Field("wang"), Field("18")}
	r2 := Row{Field("li"), Field("20")}
	rows = append(rows, r1, r2)
	t1.save(rows)

	// WHEN
	loadSchemes()
	load()

	// THEN
	t2, ok := tables["testuser5"]
	if !ok {
		t.Errorf("table testuser5 is not loaded")
	}
	if t2.Name != "testuser5" {
		t.Errorf("table name is not correct")
	}
	if t2.Rows[0][0] != "wang" {
		t.Errorf("table row field is not correct")
	}
	if t2.Rows[0][1] != "18" {
		t.Errorf("table row field is not correct")
	}
	if t2.Rows[1][0] != "li" {
		t.Errorf("table row field is not correct")
	}
	if t2.Rows[1][1] != "20" {
		t.Errorf("table row field is not correct")
	}
}

func TestSaveRowsAndSearchWithIndex(t *testing.T) {
	// GIVEN
	t1 := Table{
		Name: "testuser6",
		Columns: []ast.Column{
			{Name: "name", Kind: ast.ColumnKindText},
			{Name: "age", Kind: ast.ColumnKindInt},
		},
	}
	t1.createIndex()
	t1.saveScheme()

	// WHEN
	rows := make([]Row, 0, 4)
	r1 := Row{Field("wang"), Field("18")}
	r2 := Row{Field("li"), Field("20")}
	r3 := Row{Field("zhao"), Field("28")}
	r4 := Row{Field("qian"), Field("30")}
	rows = append(rows, r1, r2, r3, r4)
	_, err := t1.save(rows)

	// THEN
	if err != nil {
		t.Errorf("failed to save rows: %s", err)
	}
	if k := t1.index.get("name"); k == nil {
		t.Errorf("index is not created")
	}
	// Now the index value is just the row index
	if k := t1.index.get("age"); k == nil {
		t.Errorf("index is not created")
	}
	if k := t1.index.search("name", "wang"); k.Value == 0 {
		t.Errorf("index search result is not correct")
	}
	if k := t1.index.search("name", "li"); k.Value == 0 {
		t.Errorf("index search result is not correct")
	}
	if k := t1.index.search("name", "zhao"); k.Value == 0 {
		t.Errorf("index search result is not correct")
	}
	if k := t1.index.search("name", "qian"); k.Value == 0 {
		t.Errorf("index search result is not correct")
	}
}

func TestConvertRow(t *testing.T) {
	// GIVEN
	t1 := Table{
		Name: "testuser7",
		Columns: []ast.Column{
			{Name: "name", Kind: ast.ColumnKindText},
			{Name: "age", Kind: ast.ColumnKindInt},
		},
	}
	t1.createIndex()
	t1.saveScheme()

	// WHEN
	row := Row{Field("wang"), Field("18")}
	record := t1.convert(row)
	name := get(record, "name")
	age := get(record, "age")
	// THEN
	if record == nil {
		t.Errorf("record should not be nil")
	}
	if record["name"] != "wang" {
		t.Errorf("record field is not correct")
	}
	if record["age"] != 18 {
		t.Errorf("record field is not correct")
	}
	if name != "wang" {
		t.Errorf("record field is not correct")
	}
	if age != "18" {
		t.Errorf("record field is not correct")
	}
}
