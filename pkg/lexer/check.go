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

func checked(cs ...Checker) bool {
	for _, c := range cs {
		if !c.Check() {
			return false
		}
	}
	return true
}
