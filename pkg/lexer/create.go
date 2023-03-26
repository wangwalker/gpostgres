package lexer

import "github.com/wangwalker/gpostgres/pkg/ast"

func tokenizeCreate(fields []string) ([]Token, error) {
	tokens := make([]Token, 0, len(fields))
	for i, t := range fields {
		token := Token{t, 0}
		if i == 2 {
			token.Kind = TokenKindTableName
			token.Value = t
			tokens = append(tokens, token)
			continue
		}
		switch t {
		case "create":
			token.Kind = TokenKindKeywordCreate
		case "table":
			token.Kind = TokenKindTable
		case "(":
			token.Kind = TokenKindLeftBracket
		case ")":
			token.Kind = TokenKindRightBracket
		case "text":
			token.Kind = TokenKindColumnKindText
		case "int":
			token.Kind = TokenKindColumnKindInt
		default:
			// otherwise, token is the column value
			token.Kind = TokenKindColumnName
			token.Value = t
		}
		tokens = append(tokens, token)
	}
	if !checked(makeCreateCheckers(tokens)...) {
		return nil, ErrQuerySyntaxInvalid
	}
	return tokens, nil
}

func composeCreateStmt(tokens []Token) (*ast.QueryStmtCreateTable, error) {
	createStmt := ast.QueryStmtCreateTable{}
	columns := make([]ast.Column, 0)
	// make sure column name and kind is in right order
	columnStack := make([]string, 0)
	var c ast.Column
	for _, t := range tokens {
		switch t.Kind {
		case TokenKindTableName:
			createStmt.Name = t.Value
		case TokenKindColumnKindText, TokenKindColumnKindInt:
			if c.Kind != 0 {
				return nil, ErrQuerySyntaxInvalid
			}
			if len(columnStack) != 1 {
				return nil, ErrQuerySyntaxInvalid
			}
			c.Kind = ast.ColumnKind(mapColumnKind(t.Kind))
			columns = append(columns, c)
			columnStack = columnStack[:0]
		case TokenKindColumnName:
			c = ast.Column{Name: ast.ColumnName(t.Value)}
			columnStack = append(columnStack, t.Value)
		}
	}
	createStmt.Columns = columns
	return &createStmt, nil
}

func mapColumnKind(k TokenKind) ast.ColumnKind {
	switch k {
	case TokenKindColumnKindText:
		return ast.ColumnKindText
	case TokenKindColumnKindInt:
		return ast.ColumnKindInt
	}
	return ast.ColumnKindUnknown
}

func makeCreateCheckers(tokens []Token) []Checker {
	kinds := 0 // the numbers of column kind tokens
	for _, n := range tokens {
		if n.Kind == TokenKindColumnKindInt || n.Kind == TokenKindColumnKindText {
			kinds += 1
		}
	}
	return []Checker{
		LengthConstraint{
			tokens: tokens,
			pairs: []CmpValuePair{
				{cmp: ast.CmpKindGte, value: 7},
			}},
		PosKindConstraint{
			tokens: tokens,
			pairs: []PosKindPair{
				{pos: 0, kind: TokenKindKeywordCreate},
				{pos: 1, kind: TokenKindTable},
				{pos: 2, kind: TokenKindTableName},
			},
		},
		KccConstraint{
			tokens: tokens,
			paris: []KindCountCmpPair{
				{TokenKindKeywordCreate, 1, ast.CmpKindEq},
				{TokenKindTableName, 1, ast.CmpKindEq},
				{TokenKindColumnName, kinds, ast.CmpKindEq},
				{TokenKindLeftBracket, 1, ast.CmpKindEq},
				{TokenKindRightBracket, 1, ast.CmpKindEq},
			},
		},
		OrderConstraints{
			tokens: tokens,
			pairs: []KindOrderPair{
				{TokenOrderAscend, 1, []TokenKind{TokenKindTableName, TokenKindLeftBracket}},
				{TokenOrderAscend, 0, []TokenKind{TokenKindLeftBracket, TokenKindRightBracket}},
			},
		},
	}
}
