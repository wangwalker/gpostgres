package lexer

import (
	"github.com/wangwalker/gpostgres/pkg/ast"
)

type selectState uint

const (
	startingColumns selectState = iota + 1
	finishingColumns
	finishingTableName
	startingComparing
	unknown
)

// for this query: SELECT ... FROM fdt WHERE c1 > 5
// the state is changing in this way: SELECT [1] ... [2] FROM fdt [3] WHERE c1 > [4] 5
// 1,2,3,4 means the first four items of selectState
func currentState(tokens []Token) selectState {
	hasLeftBracket, hasRightBracket := false, false
	hasTableName, hasCmp := false, false
	for _, t := range tokens {
		switch t.Kind {
		case TokenKindLeftBracket:
			hasLeftBracket = true
		case TokenKindRightBracket:
			hasRightBracket = true
		case TokenKindTableName:
			hasTableName = true
		case TokenKindCmpEq, TokenKindCmpNotEq, TokenKindCmpGt,
			TokenKindCmpGte, TokenKindCmpLt, TokenKindCmpLte:
			hasCmp = true
		case TokenKindAsterisk:
			hasLeftBracket = true
			hasRightBracket = true
		}
	}
	if hasLeftBracket && hasRightBracket && hasTableName && hasCmp {
		return startingComparing
	} else if hasLeftBracket && hasRightBracket && hasTableName {
		return finishingTableName
	} else if hasLeftBracket && hasRightBracket {
		return finishingColumns
	} else if hasLeftBracket {
		return startingColumns
	}
	return unknown
}

func tokenizeSelect(fields []string) ([]Token, error) {
	tokens := make([]Token, 0, len(fields))

	for _, t := range fields {
		token := Token{t, 0}
		switch t {
		case "select":
			token.Kind = TokenKindKeywordSelect
		case "*":
			token.Kind = TokenKindAsterisk
		case "(":
			token.Kind = TokenKindLeftBracket
		case ")":
			token.Kind = TokenKindRightBracket
		case "from":
			token.Kind = TokenKindFrom
		case "where":
			token.Kind = TokenKindWhere
		case "==":
			token.Kind = TokenKindCmpEq
		case "!=":
			token.Kind = TokenKindCmpNotEq
		case ">":
			token.Kind = TokenKindCmpGt
		case ">=":
			token.Kind = TokenKindCmpGte
		case "<":
			token.Kind = TokenKindCmpLt
		case "<=":
			token.Kind = TokenKindCmpLte
		default:
			switch currentState(tokens) {
			case startingColumns:
				token.Kind = TokenKindColumnName
			case finishingColumns:
				token.Kind = TokenKindTableName
			case finishingTableName:
				token.Kind = TokenKindCmpLeft
			case startingComparing:
				token.Kind = TokenKindCmpRight
			}
		}
		tokens = append(tokens, token)
	}
	if !checked(makeSelectCheckers(tokens)...) {
		return nil, ErrQuerySyntaxInvalid
	}
	return tokens, nil
}

func composeSelectStmt(tokens []Token) (*ast.QueryStmtSelectValues, error) {
	stmt := ast.QueryStmtSelectValues{}
	columnNames := make([]ast.ColumnName, 0)
	whereClause := ast.WhereClause{}
	for _, t := range tokens {
		switch t.Kind {
		case TokenKindTableName:
			stmt.TableName = t.Value
		case TokenKindAsterisk:
			stmt.ContainsAllColumns = true
		case TokenKindColumnName:
			columnNames = append(columnNames, ast.ColumnName(t.Value))
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
	stmt.ColumnNames = columnNames
	stmt.Where = whereClause
	return &stmt, nil
}

func makeSelectCheckers(tokens []Token) []Checker {
	orderPairs := []KindOrderPair{
		{TokenOrderAscend, 1, []TokenKind{TokenKindFrom, TokenKindTableName}},
	}
	if containsKind(tokens, TokenKindWhere) {
		whereOrders := []KindOrderPair{
			{TokenOrderAscend, 0, []TokenKind{TokenKindLeftBracket, TokenKindRightBracket}},
			{TokenOrderAscend, 2, []TokenKind{TokenKindCmpLeft, TokenKindCmpRight}},
		}
		orderPairs = append(orderPairs, whereOrders...)
	}
	return []Checker{
		LengthConstraint{
			tokens: tokens,
			pairs: []CmpValuePair{
				{cmp: ast.CmpKindGte, value: 4},
			}},
		PosKindConstraint{
			tokens: tokens,
			pairs: []PosKindPair{
				{pos: 0, kind: TokenKindKeywordSelect},
			},
		},
		KccConstraint{
			tokens: tokens,
			paris: []KindCountCmpPair{
				{TokenKindKeywordSelect, 1, ast.CmpKindEq},
				{TokenKindTableName, 1, ast.CmpKindEq},
				{TokenKindLeftBracket, 1, ast.CmpKindLte},
				{TokenKindRightBracket, 1, ast.CmpKindLte},
				{TokenKindCmpLeft, 1, ast.CmpKindLte},
				{TokenKindCmpRight, 1, ast.CmpKindLte},
				{TokenKindAsterisk, 1, ast.CmpKindLte},
				{TokenKindFrom, 1, ast.CmpKindEq},
				{TokenKindWhere, 1, ast.CmpKindLte},
				{TokenKindCmpEq, 1, ast.CmpKindLte},
				{TokenKindCmpGt, 1, ast.CmpKindLte},
				{TokenKindCmpLt, 1, ast.CmpKindLte},
			},
		},
		OrderConstraints{
			tokens: tokens,
			pairs:  orderPairs,
		},
	}
}
