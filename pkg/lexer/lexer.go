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
	QuerySyntaxBracketIncompleteError = errors.New("missing ( or )")
	QuerySyntaxInvalidError           = errors.New("syntax is wrong")
	EmptyCreateTableStmt              = ast.QueryStmtCreateTable{}
)

// Create Table users ( name Text age Text );
func tokenize(source string) ([]Token, error) {
	stokens := strings.Fields(clean(source))
	tokens := make([]Token, 0, len(stokens))
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
	if !hasLeftBracket || !hasRightBracket {
		return nil, QuerySyntaxBracketIncompleteError
	}
	return tokens, nil
}

func Lex(source string) (ast.QueryStmtCreateTable, error) {
	tokens, err := tokenize(source)
	if err != nil {
		return EmptyCreateTableStmt, err
	}
	if len(tokens) < 1 {
		return EmptyCreateTableStmt, QuerySyntaxInvalidError
	}

	switch tokens[0].Kind {
	case TokenKindKeywordCreate:
		if len(tokens) < 3 {
			return EmptyCreateTableStmt, QuerySyntaxInvalidError
		}
		createStmt, err := composeCreateStmt(tokens)
		return createStmt, err
	}
	return EmptyCreateTableStmt, QuerySyntaxInvalidError
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
	var column ast.Column
	for _, t := range tokens {
		switch t.Kind {
		case TokenKindTableName:
			createStmt.Name = t.Value
		case TokenKindColumnKindText, TokenKindColumnKindInt:
			column.Kind = ast.ColumnKind(mapColumnKind(t.Kind))
			columns = append(columns, column)
		case TokenKindColumnName:
			column = ast.Column{Name: ast.ColumnName(t.Value)}
			columnCount += 1
		}
	}
	// only have both column value and kind like `name Text`, the column is valid
	if column.Kind == 0 || column.Name == "" {
		return EmptyCreateTableStmt, QuerySyntaxInvalidError
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
