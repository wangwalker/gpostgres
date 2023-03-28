package lexer

import (
	"fmt"
	"strings"

	"github.com/wangwalker/gpostgres/pkg/ast"
)

// for this query: UPDATE mytable SET a = 5, b = 3, c = 1 WHERE a > 0;
func tokenizeUpdate(fields []string) ([]Token, error) {
	tokens := make([]Token, 0, len(fields))
	// cnv : column name value
	composingCnv, finishCnv := false, false
	waitingValue, finishCmp := false, false
	var columnName string
	for i, t := range fields {
		token := Token{t, 0}
		if i == 1 {
			token.Kind = TokenKindTableName
			token.Value = t
			tokens = append(tokens, token)
			continue
		}
		if waitingValue && t != "=" {
			token.Kind = TokenKindColumnNameValue
			token.Value = fmt.Sprintf("%s=%s", columnName, t)
			tokens = append(tokens, token)
			waitingValue = false
			continue
		}
		switch t {
		case "update":
			token.Kind = TokenKindKeywordUpdate
		case "set":
			token.Kind = TokenKindSet
			composingCnv = true
		case "where":
			token.Kind = TokenKindWhere
			finishCnv = true
		case "=":
			continue
		case "==":
			token.Kind = TokenKindCmpEq
			finishCmp = true
		case "!=":
			token.Kind = TokenKindCmpNotEq
			finishCmp = true
		case ">":
			token.Kind = TokenKindCmpGt
			finishCmp = true
		case ">=":
			token.Kind = TokenKindCmpGte
			finishCmp = true
		case "<":
			token.Kind = TokenKindCmpLt
			finishCmp = true
		case "<=":
			token.Kind = TokenKindCmpLte
			finishCmp = true
		default:
			if finishCmp {
				token.Kind = TokenKindCmpRight
				token.Value = t
			} else if finishCnv {
				token.Kind = TokenKindCmpLeft
				token.Value = t
			} else if composingCnv {
				columnName = t
				waitingValue = true
				continue
			}
		}
		tokens = append(tokens, token)
	}
	if !checked(makeUpdateCheckers(tokens)...) {
		return nil, ErrQuerySyntaxInvalid
	}
	return tokens, nil
}

func composeUpdateStmt(tokens []Token) (*ast.QueryStmtUpdateValues, error) {
	stmt := ast.QueryStmtUpdateValues{}
	updatedValues := make([]ast.ColumnUpdatedValue, 0)
	whereClause := ast.WhereClause{}
	for _, t := range tokens {
		switch t.Kind {
		case TokenKindTableName:
			stmt.TableName = t.Value
		case TokenKindColumnNameValue:
			fields := strings.Split(t.Value, "=")
			cnv := ast.ColumnUpdatedValue{Name: ast.ColumnName(fields[0]), Value: fields[1]}
			updatedValues = append(updatedValues, cnv)
		case TokenKindCmpLeft:
			whereClause.Column = ast.ColumnName(t.Value)
		case TokenKindCmpRight:
			whereClause.Value = t.Value
		case TokenKindCmpEq:
			whereClause.Cmp = ast.CmpKindEq
		case TokenKindCmpNotEq:
			whereClause.Cmp = ast.CmpKindNotEq
		case TokenKindCmpGt:
			whereClause.Cmp = ast.CmpKindGt
		case TokenKindCmpGte:
			whereClause.Cmp = ast.CmpKindGte
		case TokenKindCmpLt:
			whereClause.Cmp = ast.CmpKindLt
		case TokenKindCmpLte:
			whereClause.Cmp = ast.CmpKindLte
		}
	}
	if whereClause.EitherEmpty() {
		return nil, ErrQuerySyntaxWhereIncomplete
	}
	stmt.Values = updatedValues
	stmt.Where = whereClause
	return &stmt, nil
}

func makeUpdateCheckers(tokens []Token) []Checker {
	return []Checker{
		LengthConstraint{
			tokens: tokens,
			pairs: []CmpValuePair{
				{cmp: ast.CmpKindGte, value: 8},
			}},
		PosKindConstraint{
			tokens: tokens,
			pairs: []PosKindPair{
				{pos: 0, kind: TokenKindKeywordUpdate},
				{pos: 1, kind: TokenKindTableName},
				{pos: 2, kind: TokenKindSet},
			},
		},
		KccConstraint{
			tokens: tokens,
			paris: []KindCountCmpPair{
				{TokenKindKeywordUpdate, 1, ast.CmpKindEq},
				{TokenKindTableName, 1, ast.CmpKindEq},
				{TokenKindCmpLeft, 1, ast.CmpKindLte},
				{TokenKindCmpRight, 1, ast.CmpKindLte},
				{TokenKindColumnNameValue, 1, ast.CmpKindGte},
				{TokenKindWhere, 1, ast.CmpKindLte},
				{TokenKindCmpEq, 1, ast.CmpKindLte},
				{TokenKindCmpGt, 1, ast.CmpKindLte},
				{TokenKindCmpLt, 1, ast.CmpKindLte},
			},
		},
		OrderConstraints{
			tokens: tokens,
			pairs: []KindOrderPair{
				{TokenOrderAscend, 1, []TokenKind{TokenKindKeywordUpdate, TokenKindTableName}},
				{TokenOrderAscend, 1, []TokenKind{TokenKindTableName, TokenKindSet}},
				{TokenOrderAscend, 2, []TokenKind{TokenKindCmpLeft, TokenKindCmpRight}},
			},
		},
	}
}
