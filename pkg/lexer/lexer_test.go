package lexer

import (
	"testing"

	"github.com/wangwalker/gpostgres/pkg/ast"
)

func TestCreateTableFailed(t *testing.T) {
	createTests := []struct {
		source string
	}{
		{source: "create table users"},
		{source: "create table users from"},
		{source: "create table users ((name text)"},
		{source: "create table users (name text))"},
		{source: "create table users ((name text))"},
		{source: "create table users (text name);"},
		{source: "create table users1 user2 (name text)"},
		{source: "create table users2 (name text, text, age int);"},
		{source: "create table users2 (name text, age int, gender);"},
	}

	for i, tt := range createTests {
		_, err := Lex(tt.source)
		if err == nil {
			t.Errorf("TestCreateTableFailed: test %d should create table failed, but err is null", i)
		}
	}
}

func TestCreateTableSuccessfully(t *testing.T) {
	createTests := []struct {
		source  string
		stmt    ast.QueryStmtCreateTable
		columns int
	}{
		{source: "create table users1 (name text)", stmt: ast.QueryStmtCreateTable{Name: "users1"}, columns: 1},
		{source: "create table users2 (name int);", stmt: ast.QueryStmtCreateTable{Name: "users2"}, columns: 1},
		{source: "create table users3 ( name text );", stmt: ast.QueryStmtCreateTable{Name: "users3"}, columns: 1},
		{source: "create table cities (a text, b text);", stmt: ast.QueryStmtCreateTable{Name: "cities"}, columns: 2},
		{source: "create table cities2 (a text, b int, c text);", stmt: ast.QueryStmtCreateTable{Name: "cities2"}, columns: 3},
	}

	for i, tt := range createTests {
		stmt, err := Lex(tt.source)
		if err != nil {
			t.Errorf("TestCreateTableSuccessfully: test %d should create table successfully, but err is not null: %v", i, err)
		}
		create, ok := stmt.(*ast.QueryStmtCreateTable)
		if !ok {
			t.Errorf("TestCreateTableSuccessfully: test %d should create table successfully, but return wrong type", i)
		}
		if tt.stmt.Name != create.Name {
			t.Errorf("TestCreateTableSuccessfully: test %d should create table successfully, but name is not equal", i)
		}
		if tt.columns != len(create.Columns) {
			t.Errorf("TestCreateTableSuccessfully: test %d should create table successfully, but get wrong columns", i)
		}
	}
}

func TestInsertFailedWhenTableNotExisted(t *testing.T) {
	insertTests := []struct {
		source string
	}{
		{source: "insert"},
		{source: "update"},
		{source: "insert table"},
		{source: "insert into users"},
		{source: "insert into users values"},
		{source: "insert into users (name text)"},
	}
	for i, tt := range insertTests {
		_, err := Lex(tt.source)
		if err == nil {
			t.Errorf("TestInsertFailedWhenTableNotExisted: test %d should fail, but err is null", i)
		}
	}
}

func TestInsertFailledWhenSyntaxWrong(t *testing.T) {
	// Create a table named users
	_, err := Lex("create table u (name text, age int);")
	if err != nil {
		t.Errorf("TestInsertFailledWhenSyntaxWrong: should create table successfully, but err: %v", err)
	}

	insertTests := []string{
		"insert into us ('a', 11)",
		"create into u values ('a', 11)",
		"insert into u values ('a')",
		"insert into u values ('a', 'b', 'c');",
		"insert into u values )'a', 11(",
		"insert into u (n, a) values ()",
		"insert into u (n, a) values ('a', 2)",
		"insert into u (name, a) values ('a', 2)",
		"insert into u (name, age, gender) values ('a', 2)",
		"insert into u (name, age) values ('w', 2, 3)",
	}
	for i, tt := range insertTests {
		_, err := Lex(tt)
		if err == nil {
			t.Errorf("TestInsertFailledWhenSyntaxWrong: test %d should fail, but error is null", i)
		}
	}
}

func TestInsertSuccessfully(t *testing.T) {
	// Create a table named users
	_, err := Lex("create table users (name text, age int);")
	if err != nil {
		t.Errorf("TestInsertFailledWhenSyntaxWrong: should create table successfully, but err: %v", err)
	}

	insertTests := []string{
		"insert into users values ('a', 11)",
		"insert into users (name, age) values ('a', 11)",
		"insert into users values ('a', 11), ('b', 12);",
		"insert into users (name, age) values ('a', 11), ('b', 12);",
	}
	for i, tt := range insertTests {
		_, err := Lex(tt)
		if err != nil {
			t.Errorf("TestInsertSuccessfully: test %d should succeed, but error is not null: %v", i, err)
		}
	}
}
