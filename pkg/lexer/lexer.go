package lexer

import (
	"errors"
	"strings"
)

type TokenKind uint

const (
	TokenKindKeyword = iota
	TokenKindTable
)

type Keyword string

const (
	KeywordCreate = "create"
	KeywordSelect = "select"
	KeywordAlter  = "alter"
	KeywordDrop   = "drop"
)

type Token struct {
	Value string
	Kind  TokenKind
}

func tokenize(source string) []Token {
	stokens := strings.Split(source, " ")
	tokens := make([]Token, len(stokens))
	for _, t := range stokens {
		token := Token{t, 0}
		switch t {
		case "create":
			token.Kind = TokenKindKeyword
			token.Value = KeywordCreate
		case "table":
			token.Kind = TokenKindTable
		}
		tokens = append(tokens, token)
	}
	return tokens[len(stokens):]
}

type QueryStmtKind uint

const (
	QueryStmtKindCreate = iota
	QueryStmtKindEmpty
	QueryStmtKindUnkown
)

type QueryStmtCreateTable struct {
	Name Token
}

func Lex(source string) (QueryStmtCreateTable, error) {
	tokens := tokenize(source)
	if len(tokens) < 1 {
		return QueryStmtCreateTable{}, errors.New("information is not enough")
	}

	createStmt := QueryStmtCreateTable{}

	switch tokens[0].Kind {
	case TokenKindKeyword:
		if len(tokens) > 2 {
			createStmt.Name = tokens[2]
			return createStmt, nil
		}
		return QueryStmtCreateTable{}, errors.New("wrong create")
	}
	return QueryStmtCreateTable{}, errors.New("unknown")
}
