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
	TokenKindColumnKeyText
	TokenKindColumnValue
)

type Token struct {
	Value string
	Kind  TokenKind
}

var (
	QueryInfoEmptyError               = errors.New("information is not enough")
	QueryCreateInfoIncompleteError    = errors.New("create table is not enough")
	QuerySyntaxBracketIncompleteError = errors.New("missing ( or )")
	QuerySyntaxUnkownError            = errors.New("syntax is wrong")
)

// Create Table users ( name Text age Text );
func tokenize(source string) ([]Token, error) {
	stokens := strings.Fields(clean(source))
	tokens := make([]Token, len(stokens))
	isCreateQuery := false
	hasLeftBracket := false
	hasRightBracket := false
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
			hasLeftBracket = true
		case ")":
			token.Kind = TokenKindRightBracket
			hasRightBracket = true
		case "text":
			token.Kind = TokenKindColumnKeyText
		default:
			// otherwise, token is the column value
			token.Kind = TokenKindColumnValue
			token.Value = t
		}
		tokens = append(tokens, token)
	}
	if !hasLeftBracket || !hasRightBracket {
		return nil, QuerySyntaxBracketIncompleteError
	}
	return tokens[len(stokens):], nil
}

func Lex(source string) (ast.QueryStmtCreateTable, error) {
	tokens, err := tokenize(source)
	if err != nil {
		return ast.QueryStmtCreateTable{}, err
	}
	if len(tokens) < 1 {
		return ast.QueryStmtCreateTable{}, QueryInfoEmptyError
	}

	switch tokens[0].Kind {
	case TokenKindKeywordCreate:
		if len(tokens) < 3 {
			return ast.QueryStmtCreateTable{}, QueryCreateInfoIncompleteError
		}
		createStmt, err := composeCreateStmt(tokens)
		return createStmt, err
	}
	return ast.QueryStmtCreateTable{}, QuerySyntaxUnkownError
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
	columnCount := 0
	columns := make([]ast.Column, 0)
	columnPaired := false
	for _, t := range tokens {
		switch t.Kind {
		case TokenKindTableName:
			createStmt.Name = t.Value
		case TokenKindColumnKeyText:
			columnPaired = true
		case TokenKindColumnValue:
			columnCount += 1
			column := ast.Column{Kind: ast.ColumnKindText, Value: ast.ColumnValue(t.Value)}
			columns = append(columns, column)
			columnPaired = false
		}
	}
	// only have both column value and kind like `name Text`, the column is valid
	if !columnPaired {
		return ast.QueryStmtCreateTable{}, errors.New("wrong create table")
	}
	createStmt.Columns = columns[:columnCount]
	return createStmt, nil
}
