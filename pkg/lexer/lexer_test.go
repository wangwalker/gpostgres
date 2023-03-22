package lexer

import (
	"testing"

	"github.com/wangwalker/gpostgres/pkg/ast"
)

func TestCreateTableFailed(t *testing.T) {
	tokensTests := []struct {
		source string
		stmt   ast.QueryStmtCreateTable
	}{
		{source: "create table users", stmt: ast.QueryStmtCreateTable{Name: "users"}},
		{source: "create table users from", stmt: ast.QueryStmtCreateTable{Name: "users"}},
		{source: "create table users ((name text)", stmt: ast.QueryStmtCreateTable{Name: "users"}},
		{source: "create table users (name text))", stmt: ast.QueryStmtCreateTable{Name: "users"}},
		{source: "create table users ((name text))", stmt: ast.QueryStmtCreateTable{Name: "users"}},
		{source: "create table users1 user2 (name text)", stmt: ast.QueryStmtCreateTable{Name: "users"}},
		{source: "create table users2 (name text, text, age int);", stmt: ast.QueryStmtCreateTable{Name: "users2"}},
		{source: "create table users2 (name text, age int, gender);", stmt: ast.QueryStmtCreateTable{Name: "users2"}},
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
		{source: "create table users (name text);", stmt: ast.QueryStmtCreateTable{Name: "users"}, columns: 1},
		{source: "create table cities (a text, b text);", stmt: ast.QueryStmtCreateTable{Name: "cities"}, columns: 2},
		{source: "create table cities2 (a text, b int, c text);", stmt: ast.QueryStmtCreateTable{Name: "cities2"}, columns: 3},
	}

	for i, tt := range tokensTests {
		table, err := Lex(tt.source)
		nameEqual := table.Name == tt.stmt.Name
		columnsEqual := len(table.Columns) == tt.columns
		if !nameEqual || err != nil || !columnsEqual {
			t.Errorf("TestLexCreateTable: source index %d should create table successfully, but is not null or name is not equal", i)
		}
	}
}
