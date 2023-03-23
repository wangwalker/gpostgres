package lexer

import (
	"testing"

	"github.com/wangwalker/gpostgres/pkg/ast"
)

func TestCreateTableFailed(t *testing.T) {
	tokensTests := []struct {
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

	for i, tt := range tokensTests {
		_, err := Lex(tt.source)
		if err == nil {
			t.Errorf("TestLexCreateTable: source index %d should create table failed, but err is null", i)
		}
	}
}

func TestCreateTableSuccessfully(t *testing.T) {
	tokensTests := []struct {
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

	for i, tt := range tokensTests {
		stmt, err := Lex(tt.source)
		if err != nil {
			t.Errorf("TestLexCreateTable: source index %d should create table successfully, but err is not null: %v", i, err)
		}
		create, ok := stmt.(*ast.QueryStmtCreateTable)
		if !ok {
			t.Errorf("TestLexCreateTable: source index %d should create table successfully, but return wrong type", i)
		}
		if tt.stmt.Name != create.Name {
			t.Errorf("TestLexCreateTable: source index %d should create table successfully, but name is not equal", i)
		}
		if tt.columns != len(create.Columns) {
			t.Errorf("TestLexCreateTable: source index %d should create table successfully, but get wrong columns", i)
		}
	}
}
