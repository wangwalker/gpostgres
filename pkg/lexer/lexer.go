package lexer

import (
	"errors"
	"strings"

	"github.com/wangwalker/gpostgres/pkg/ast"
)

type TokenKind uint

const (
	TokenKindKeywordCreate TokenKind = iota
	TokenKindKeywordSelect
	TokenKindKeywordAlter
	TokenKindKeywordDrop
	TokenKindTable
	TokenKindTableName
	TokenKindLeftBracket
	TokenKindRightBracket
	TokenKindColumnKindText
	TokenKindColumnKindInt
	TokenKindColumnName
)

type Token struct {
	Value string
	Kind  TokenKind
}

var (
	ErrQuerySyntaxBracketIncomplete = errors.New("missing ( or )")
	ErrQuerySyntaxInvalid           = errors.New("syntax is wrong")
	EmptyCreateTableStmt            = ast.QueryStmtCreateTable{}
)

// Create Table users ( name Text age Text );
func tokenize(source string) ([]Token, error) {
	stokens := strings.Fields(clean(source))
	tokens := make([]Token, 0, len(stokens))
	brackets := make([]string, 0)
	isCreateQuery := false
	for i, t := range stokens {
		token := Token{t, 0}
		if isCreateQuery && i == 2 {
			token.Kind = TokenKindTableName
			token.Value = t
			tokens = append(tokens, token)
			continue
		}
		switch t {
		case "create":
			token.Kind = TokenKindKeywordCreate
			isCreateQuery = true
		case "table":
			token.Kind = TokenKindTable
		case "(":
			token.Kind = TokenKindLeftBracket
			brackets = append(brackets, t)
		case ")":
			token.Kind = TokenKindRightBracket
			count := len(brackets)
			if count != 1 {
				return nil, ErrQuerySyntaxBracketIncomplete
			}
			brackets = brackets[:count-1]
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
	if len(brackets) != 0 {
		return nil, ErrQuerySyntaxBracketIncomplete
	}

	return tokens, nil
}

func Lex(source string) (ast.QueryStmtCreateTable, error) {
	tokens, err := tokenize(source)
	if err != nil {
		return EmptyCreateTableStmt, err
	}
	if len(tokens) < 1 {
		return EmptyCreateTableStmt, ErrQuerySyntaxInvalid
	}

	switch tokens[0].Kind {
	case TokenKindKeywordCreate:
		if len(tokens) <= 3 {
			return EmptyCreateTableStmt, ErrQuerySyntaxInvalid
		}
		createStmt, err := composeCreateStmt(tokens)
		if err != nil {
			return EmptyCreateTableStmt, err
		}
		return createStmt, nil
	}
	return EmptyCreateTableStmt, ErrQuerySyntaxInvalid
}

func clean(souce string) string {
	s := strings.Replace(souce, ";", "", 1)
	s = strings.ReplaceAll(s, ",", " ")
	s = strings.ReplaceAll(s, "(", " ( ")
	s = strings.ReplaceAll(s, ")", " ) ")
	return s
}

func composeCreateStmt(tokens []Token) (ast.QueryStmtCreateTable, error) {
	createStmt := ast.QueryStmtCreateTable{}
	columns := make([]ast.Column, 0)
	var c ast.Column
	names, kinds := 0, 0
	for _, t := range tokens {
		switch t.Kind {
		case TokenKindTableName:
			createStmt.Name = t.Value
		case TokenKindColumnKindText, TokenKindColumnKindInt:
			if c.Kind != 0 {
				return EmptyCreateTableStmt, ErrQuerySyntaxInvalid
			}
			c.Kind = ast.ColumnKind(mapColumnKind(t.Kind))
			columns = append(columns, c)
			kinds += 1
		case TokenKindColumnName:
			c = ast.Column{Name: ast.ColumnName(t.Value)}
			names += 1
		}
	}
	if names != kinds {
		return EmptyCreateTableStmt, ErrQuerySyntaxInvalid
	}
	createStmt.Columns = columns
	return createStmt, nil
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
