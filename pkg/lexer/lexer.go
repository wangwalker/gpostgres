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
	switch stokens[0] {
	case "create":
		return tokenizeCreate(stokens)
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
