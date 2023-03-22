package lexer

import (
	"errors"
	"fmt"
	"strings"

	"github.com/wangwalker/gpostgres/pkg/ast"
	"github.com/wangwalker/gpostgres/pkg/storage"
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
)

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
				return nil, ErrQuerySyntaxInvalid
			}
			brackets = brackets[:0]
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

func Lex(source string) (*ast.QueryStmtCreateTable, error) {
	tokens, err := tokenize(source)
	if err != nil {
		return nil, err
	}
	if len(tokens) < 1 {
		return nil, ErrQuerySyntaxInvalid
	}

	switch tokens[0].Kind {
	case TokenKindKeywordCreate:
		// The shortest creation has 7 tokens like: CREATE TABLE users (name text)
		if len(tokens) < 7 {
			return nil, ErrQuerySyntaxInvalid
		}
		createStmt, err := composeCreateStmt(tokens)
		if err != nil {
			return nil, err
		}
		err = storage.CreateTable(createStmt)
		if err != nil {
			return nil, err
		}
		fmt.Printf("create table: %s OK!\n", createStmt.Name)
		return createStmt, nil
	}
	return nil, nil
}

func clean(souce string) string {
	s := strings.Replace(souce, ";", "", 1)
	s = strings.ReplaceAll(s, ",", " ")
	s = strings.ReplaceAll(s, "(", " ( ")
	s = strings.ReplaceAll(s, ")", " ) ")
	return s
}

func composeCreateStmt(tokens []Token) (*ast.QueryStmtCreateTable, error) {
	createStmt := ast.QueryStmtCreateTable{}
	columns := make([]ast.Column, 0)
	// make sure column name and kind is in right order
	columnStack := make([]string, 0)
	var c ast.Column
	// keep track of the numbers of column names and column kinds, make sure they are equal
	names, kinds := 0, 0
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
			kinds += 1
		case TokenKindColumnName:
			c = ast.Column{Name: ast.ColumnName(t.Value)}
			columnStack = append(columnStack, t.Value)
			names += 1
		}
	}
	if names != kinds {
		return nil, ErrQuerySyntaxInvalid
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
