package ast

type QueryStmtKind uint

const (
	QueryStmtKindCreate = iota
	QueryStmtKindEmpty
	QueryStmtKindUnkown
)

type ColumnName string
type ColumnKind uint8
type ColumnValue string
type Row []ColumnValue

func (c ColumnKind) String() string {
	switch c {
	case ColumnKindInt:
		return "Int"
	case ColumnKindText:
		return "Text"
	}
	return ""
}

type Column struct {
	Name ColumnName
	Kind ColumnKind
}

const (
	ColumnKindText ColumnKind = iota + 1
	ColumnKindInt
	ColumnKindUnknown
)

type QueryStmtCreateTable struct {
	Name    string // TableName
	Columns []Column
}

type QueryStmtInsertValues struct {
	TableName          string
	ColumnNames        []ColumnName
	Rows               []Row
	ContainsAllColumns bool
}
