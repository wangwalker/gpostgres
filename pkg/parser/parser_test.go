package parser

import "testing"

func TestPrepare(t *testing.T) {
	queryTests := []struct {
		query  string
		result QueryType
	}{
		{"", QueryTypeUnkown},
		{"\\q", QueryTypeCommand},
		{"\\quit", QueryTypeCommand},
		{"\\h", QueryTypeCommand},
		{"\\help", QueryTypeCommand},
		{"\\d", QueryTypeCommand},
		{"\\d table", QueryTypeCommand},
		{"dd", QueryTypeUnkown},

		{"create table users", QueryTypeUnkown},
		{"create table users (name text)", QueryTypeUnkown},
		{"create table users (name text);", QueryTypeNormal},
		{"create table users (name text, age int);", QueryTypeNormal},
	}

	for _, tt := range queryTests {
		result := Prepare(tt.query)
		if result != tt.result {
			t.Errorf("input query %s should be %v, but got %v", tt.query, tt.result, result)
		}
	}
}

func TestParseCommand(t *testing.T) {
	cmdTests := []struct {
		cmd     string
		cmdType CommandType
	}{
		{"", UnknownCommand},
		{"q", QuitCommand},
		{"\\q", QuitCommand},
		{"quit", QuitCommand},
		{"\\quit", QuitCommand},
		{"h", HelpCommand},
		{"help", HelpCommand},
		{"\\help", HelpCommand},
		{"\\d", SchemeCommand},
		{"d", SchemeCommand},
		{"\\d table", SchemeCommand},
		{"\\d table name", SchemeCommand},
	}

	for _, tt := range cmdTests {
		cmdTpe, _ := ParseCommand(tt.cmd)
		if cmdTpe != tt.cmdType {
			t.Errorf("input string %s should be command %v, but got result %v", tt.cmd, tt.cmdType, cmdTpe)
		}
	}
}
