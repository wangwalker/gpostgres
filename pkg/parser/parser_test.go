package parser

import "testing"

func TestParse(t *testing.T) {
	cmdTests := []struct {
		cmd     string
		cmdType CommandType
	}{
		{"", UnknownCommand},
		{"q", QuitCommand},
		{"quit", QuitCommand},
		{"h", HelpCommand},
		{"help", HelpCommand},
	}

	for _, tt := range cmdTests {
		cmdTpe := ParseCommand(tt.cmd)
		if cmdTpe != tt.cmdType {
			t.Errorf("input string %s should be command %v, but got result %v", tt.cmd, tt.cmdType, cmdTpe)
		}
	}
}
