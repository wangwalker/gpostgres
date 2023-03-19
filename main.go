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
		if lastInput == "" {
			fmt.Print("postgres# ")
		} else {
			fmt.Print("postgres> ")
		}

		// Scans a line from Stdin(Console)
		scanner.Scan()
		// Holds the string that scanned
		query := strings.ToLower(scanner.Text())
		if lastInput != "" {
			query = lastInput + query
			lastInput = ""
		}

		switch parser.Prepare(query) {
		case parser.QueryTypeCommand:
			switch parser.ParseCommand(query) {
			case parser.QuitCommand:
				fmt.Println("quit")
				break REPL
			case parser.HelpCommand:
				fmt.Println("help")
			case parser.UnknownCommand:
				fmt.Printf("invalid command: [%s]\n", query)
			}
		case parser.QueryTypeNormal:
			createStmt, err := lexer.Lex(query)
			if err != nil {
				fmt.Printf("Error: invalid query %s, error: %s\n", query, err)
				continue REPL
			}
			err = storage.CreateTable(createStmt)
			if err != nil {
				fmt.Printf("Error: invalid query %s, error: %s\n", query, err)
				continue REPL
			}
			fmt.Printf("Create Table: %s ok!\n", createStmt.Name)
		case parser.QueryTypeUnkown:
			// When an unexpected \n or \r is coming, holds it for next loop
			lastInput = query + " "
		}
	}
}
