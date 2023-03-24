package lexer

import (
	"github.com/wangwalker/gpostgres/pkg/ast"
)

func tokenizeInsert(stokens []string) ([]Token, error) {
	tokens := make([]Token, 0, len(stokens))
	containsAllColumns, finishColumnNames := false, false
	for i, t := range stokens {
		token := Token{t, 0}
		if i == 2 {
			token.Kind = TokenKindTableName
			token.Value = t
			tokens = append(tokens, token)
			continue
		}
		if i == 3 && t == "values" {
			containsAllColumns = true
			continue
		}
		switch t {
		case "insert":
			token.Kind = TokenKindKeywordInsert
		case "into":
			token.Kind = TokenKindInto
		case "values":
			// must be case like:  INSERT INTO products (product_no, name, price) `VALUES`` (1, 'Cheese', 9.99);
			finishColumnNames = true
		case "(":
			token.Kind = TokenKindLeftBracket
		case ")":
			token.Kind = TokenKindRightBracket
		default:
			// otherwise, token is column name or value
			if containsAllColumns {
				token.Kind = TokenKindColumnValue
				token.Value = t
			} else if !finishColumnNames {
				token.Kind = TokenKindColumnName
				token.Value = t
			} else {
				token.Kind = TokenKindColumnValue
				token.Value = t
			}
		}
		tokens = append(tokens, token)
	}
	if !checked(makeInsertCheckers(tokens)...) {
		return nil, ErrQuerySyntaxInvalid
	}
	return tokens, nil
}

func composeInsertStmt(tokens []Token) (*ast.QueryStmtInsertValues, error) {
	stmt := ast.QueryStmtInsertValues{}
	names, values := make([]ast.ColumnName, 0), make([]ast.ColumnValue, 0)
	for _, t := range tokens {
		switch t.Kind {
		case TokenKindTableName:
			stmt.TableName = t.Value
		case TokenKindColumnName:
			name := ast.ColumnName(t.Value)
			names = append(names, name)
		case TokenKindColumnValue:
			value := t.Value
			values = append(values, ast.ColumnValue(value))
		}
	}

	if len(names) == 0 {
		stmt.ContainsAllColumns = true
	}
	stmt.ColumnNames = names
	stmt.ColumnValues = values
	return &stmt, nil
}

func makeInsertCheckers(tokens []Token) []Checker {
	return []Checker{
		LengthConstraint{
			tokens: tokens,
			pairs: []CmpValuePair{
				{cmp: CmpKindGte, value: 6},
			}},
		PosKindConstraint{
			tokens: tokens,
			pairs: []PosKindPair{
				{pos: 0, kind: TokenKindKeywordInsert},
				{pos: 1, kind: TokenKindInto},
			},
		},
		KccConstraint{
			tokens: tokens,
			paris: []KindCountCmpPair{
				{TokenKindKeywordInsert, 1, CmpKindEq},
				{TokenKindInto, 1, CmpKindEq},
				{TokenKindLeftBracket, 1, CmpKindGte},
				{TokenKindRightBracket, 1, CmpKindGte},
			},
		},
	}
}
