package lexer

import (
	"testing"
)

func TestLexCreateTable(t *testing.T) {
	tokensTests := []struct {
		source string
		stmt   QueryStmtCreateTable
	}{
		{source: "create table users", stmt: QueryStmtCreateTable{Token{Value: "users", Kind: 0}}},
		{source: "create table users from", stmt: QueryStmtCreateTable{Token{Value: "users", Kind: 0}}},
	}

	for _, tt := range tokensTests {
		table, err := Lex(tt.source)
		if err != nil {
			t.Errorf("source %s after Lex() should get table name %s, but got %s",
				tt.source, tt.stmt.Name.Value, table.Name.Value)
		}
	}
}
