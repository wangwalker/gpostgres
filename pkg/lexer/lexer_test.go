package lexer

import (
	"testing"

	"github.com/wangwalker/gpostgres/pkg/ast"
)

func TestLexCreateTable(t *testing.T) {
	tokensTests := []struct {
		source string
		stmt   ast.QueryStmtCreateTable
		result bool
	}{
		{source: "create table users", stmt: ast.QueryStmtCreateTable{Name: "user"}, result: false},
		{source: "create table users from", stmt: ast.QueryStmtCreateTable{Name: "users"}, result: false},
		{source: "create table users (name text)", stmt: ast.QueryStmtCreateTable{Name: "users"}, result: false},
		{source: "create table users (name text);", stmt: ast.QueryStmtCreateTable{Name: "users"}, result: true},
		{source: "create table users (name text)", stmt: ast.QueryStmtCreateTable{Name: "users"}, result: false},
		{source: "create table cities (a text, b text);", stmt: ast.QueryStmtCreateTable{Name: "users"}, result: false},
	}

	for _, tt := range tokensTests {
		table, err := Lex(tt.source)
		if err != nil && tt.result {
			t.Errorf("source %s after Lex() should get table name %s, but got %s",
				tt.source, tt.stmt.Name, table.Name)
		}
	}
}
