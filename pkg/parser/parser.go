package parser

import (
	"strings"
)

type QueryType uint

const (
	QueryTypeCommand QueryType = iota
	QueryTypeNormal
	QueryTypeUnkown
)

type CommandType uint

const (
	QuitCommand CommandType = iota
	HelpCommand
	SchemeCommand
	UnknownCommand
)

// Prepare parses the type of incoming query, if valid, return its type, otherwise QueryTypeUnkown.
func Prepare(query string) QueryType {
	if strings.HasPrefix(query, "\\") {
		return QueryTypeCommand
	} else if strings.HasSuffix(query, ";") {
		return QueryTypeNormal
	} else {
		return QueryTypeUnkown
	}
}

// ParseCommand tests if a command is valid. If so, return the right CommandType,
// and if the command has subcommand, also return it. Otherwise, return UnknownCommand.
func ParseCommand(c string) (CommandType, string) {
	comand := strings.Replace(c, "\\", "", 1)
	comand = strings.TrimSpace(comand)
	if strings.HasPrefix(comand, "d ") {
		tn := strings.Replace(comand, "d ", "", 1)
		return SchemeCommand, strings.TrimSpace(tn)
	}
	t := UnknownCommand
	switch comand {
	case "q", "quit":
		t = QuitCommand
	case "h", "help":
		t = HelpCommand
	case "d":
		t = SchemeCommand
	}
	return t, ""
}
