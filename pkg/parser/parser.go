package parser

type CommandType uint

const (
	QuitCommand CommandType = iota
	HelpCommand
	UnknownCommand
)

func ParseCommand(c string) CommandType {
	t := UnknownCommand
	switch c {
	case "q", "quit":
		t = QuitCommand
	case "h", "help":
		t = HelpCommand
	}
	return t
}
