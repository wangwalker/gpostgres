package lexer

type CmpKind uint

const (
	CmpKindEq  CmpKind = iota // ==
	CmpKindGt                 // >
	CmpKindGte                // >=
	CmpKindLt                 // <
	CmpKindLte                // <=
)

type Checker interface {
	Check() bool
}

type CmpValuePair struct {
	cmp   CmpKind
	value int
}

// The constaints about length of tokens, such as must equal n, or greater than n
type LengthConstraint struct {
	tokens []Token
	pairs  []CmpValuePair
}

func (lc LengthConstraint) Check() bool {
	l := len(lc.tokens)
	ok := true
	for _, pair := range lc.pairs {
		switch pair.cmp {
		case CmpKindEq:
			ok = (l == pair.value) && ok
		case CmpKindGt:
			ok = (l > pair.value) && ok
		case CmpKindGte:
			ok = (l >= pair.value) && ok
		case CmpKindLt:
			ok = (l < pair.value) && ok
		case CmpKindLte:
			ok = (l <= pair.value) && ok
		}
		if !ok {
			return false
		}
	}

	return true
}

type PosKindPair struct {
	pos  int
	kind TokenKind
}

// The constraints about the nth token must be one of TokenKind
type PosKindConstraint struct {
	tokens []Token
	pairs  []PosKindPair
}

func (pk PosKindConstraint) Check() bool {
	for _, pair := range pk.pairs {
		t := pk.tokens[pair.pos]
		if t.Kind != pair.kind {
			return false
		}
	}
	return true
}

type KindCountCmpPair struct {
	kind  TokenKind
	count int
	cmp   CmpKind
}

// The constraints about the number of tokens with one specific kind must satisfy conditions
type KccConstraint struct {
	tokens []Token
	paris  []KindCountCmpPair
}

func (kcc KccConstraint) Check() bool {
	for _, pair := range kcc.paris {
		count := 0
		for _, t := range kcc.tokens {
			if t.Kind == pair.kind {
				count += 1
			}
		}
		ok := true
		switch pair.cmp {
		case CmpKindEq:
			ok = (count == pair.count) && ok
		case CmpKindGt:
			ok = (count > pair.count) && ok
		case CmpKindGte:
			ok = (count >= pair.count) && ok
		case CmpKindLt:
			ok = (count < pair.count) && ok
		case CmpKindLte:
			ok = (count <= pair.count) && ok
		}
		if !ok {
			return false
		}
	}

	return true
}

type TokenKindOrder uint

const (
	TokenOrderAscend TokenKindOrder = iota
	TokenOrderDescend
)

// Check if the given tokens has the expected order with specific kinds
// If steps is 0, just check order, don't consider the padding between them
type KindOrderPair struct {
	order TokenKindOrder
	steps int
	kinds []TokenKind // just needs two kinds
}

type OrderConstraints struct {
	tokens []Token
	pairs  []KindOrderPair
}

func (oc OrderConstraints) Check() bool {
	for _, pair := range oc.pairs {
		kind1, kind2 := pair.kinds[0], pair.kinds[1]
		idx1, idx2 := 0, 0
		for i, t := range oc.tokens {
			if t.Kind == kind1 {
				idx1 = i
			}
			if t.Kind == kind2 {
				idx2 = i
			}
		}
		switch pair.order {
		case TokenOrderAscend:
			if pair.steps == 0 {
				if idx1 > idx2 {
					return false
				}
				continue
			}
			if (idx2 - idx1) != pair.steps {
				return false
			}
		case TokenOrderDescend:
			if pair.steps == 0 {
				if idx1 < idx2 {
					return false
				}
				continue
			}
			if (idx1 - idx2) != pair.steps {
				return false
			}
		}
	}
	return true
}

func checked(cs ...Checker) bool {
	for _, c := range cs {
		if !c.Check() {
			return false
		}
	}
	return true
}
