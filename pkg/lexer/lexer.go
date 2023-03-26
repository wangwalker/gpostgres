package lexer

import (
	"errors"
	"fmt"
	"strings"

	"github.com/wangwalker/gpostgres/pkg/storage"
)

type TokenKind uint

const (
	TokenKindKeywordCreate TokenKind = iota
	TokenKindKeywordInsert
	TokenKindKeywordSelect
	TokenKindKeywordAlter
	TokenKindKeywordDrop
	TokenKindTable
	TokenKindTableName
	TokenKindInto
	TokenKindValues
	TokenKindLeftBracket
	TokenKindRightBracket
	TokenKindColumnKindText
	TokenKindColumnKindInt
	TokenKindColumnName
	TokenKindColumnValue
	TokenKindAsterisk
	TokenKindFrom
	TokenKindWhere
	TokenKindCmpEq
	TokenKindCmpNotEq
	TokenKindCmpGt
	TokenKindCmpGte
	TokenKindCmpLt
	TokenKindCmpLte
	TokenKindCmpLeft
	TokenKindCmpRight
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
	fields := strings.Fields(clean(source))
	switch fields[0] {
	case "create":
		return tokenizeCreate(fields)
	case "insert":
		return tokenizeInsert(fields)
	case "select":
		return tokenizeSelect(fields)
	}
	return nil, nil
}

func Lex(source string) (interface{}, error) {
	tokens, err := tokenize(source)
	if err != nil {
		return nil, err
	}
	if len(tokens) < 1 {
		return nil, ErrQuerySyntaxInvalid
	}

	switch tokens[0].Kind {
	case TokenKindKeywordCreate:
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
	case TokenKindKeywordInsert:
		stmt, err := composeInsertStmt(tokens)
		if err != nil {
			return nil, err
		}
		n, err := storage.Insert(stmt)
		if err != nil {
			return nil, err
		}
		fmt.Printf("insert %d rows ok!\n", n)
		return stmt, nil
	case TokenKindKeywordSelect:
		stmt, err := composeSelectStmt(tokens)
		if err != nil {
			return nil, err
		}
		rows, err := storage.Select(stmt)
		if err != nil {
			return nil, err
		}
		fmt.Printf("select %d rows ok!\n%v\n", len(rows), rows)
		return rows, nil
	}
	return nil, ErrQuerySyntaxInvalid
}

func clean(souce string) string {
	s := strings.Replace(souce, ";", "", 1)
	s = strings.ReplaceAll(s, ",", " ")
	s = strings.ReplaceAll(s, "(", " ( ")
	s = strings.ReplaceAll(s, ")", " ) ")
	return s
}

func containsKind(tokens []Token, kind TokenKind) bool {
	for _, t := range tokens {
		if t.Kind == kind {
			return true
		}
	}
	return false
}
