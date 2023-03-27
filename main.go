package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/wangwalker/gpostgres/pkg/lexer"
	"github.com/wangwalker/gpostgres/pkg/parser"
	"github.com/wangwalker/gpostgres/pkg/storage"
)

func main() {
	lastInput := ""
	scanner := bufio.NewScanner(os.Stdin)
REPL:
	for {
		lastInput = strings.TrimSpace(lastInput)
		if lastInput == "" {
			fmt.Print("postgres# ")
		} else {
			fmt.Print("postgres> ")
		}

		scanner.Scan()
		query := strings.ToLower(scanner.Text())
		if lastInput != "" {
			query = lastInput + query
			lastInput = ""
		}

		switch parser.Prepare(query) {
		case parser.QueryTypeCommand:
			commandType, table := parser.ParseCommand(query)
			switch commandType {
			case parser.QuitCommand:
				showCommand(commandType, "")
				break REPL
			case parser.SchemeCommand:
				storage.ShowTableSchemes(table)
				showCommand(commandType, "")
			case parser.HelpCommand, parser.UnknownCommand:
				showCommand(commandType, "")
			}
		case parser.QueryTypeNormal:
			_, err := lexer.Lex(query)
			if err != nil {
				fmt.Printf("Error: %s\n", err)
			}
		case parser.QueryTypeUnkown:
			// When an unexpected \n or \r is coming, holds it for next loop
			lastInput = query + " "
		}
	}
}

func showCommand(c parser.CommandType, info string) {
	switch c {
	case parser.HelpCommand:
		fmt.Println("help")
	case parser.QuitCommand:
		fmt.Println("quit")
	case parser.SchemeCommand:
		fmt.Println(info)
	case parser.UnknownCommand:
		fmt.Printf("invalid query: %s\n", info)
	}
}
