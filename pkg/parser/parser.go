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
	UnknownCommand
)

func Prepare(query string) QueryType {
	if strings.HasPrefix(query, "\\") {
		return QueryTypeCommand
	} else if strings.HasSuffix(query, ";") {
		return QueryTypeNormal
	} else {
		return QueryTypeUnkown
	}
}

func ParseCommand(c string) CommandType {
	comand := strings.Replace(c, "\\", "", 1)
	comand = strings.TrimSpace(comand)
	t := UnknownCommand
	switch comand {
	case "q", "quit":
		t = QuitCommand
	case "h", "help":
		t = HelpCommand
	}
	return t
}
