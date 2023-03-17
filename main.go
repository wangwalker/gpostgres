package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/wangwalker/gpostgres/pkg/lexer"
	"github.com/wangwalker/gpostgres/pkg/parser"
)

const (
	Prompt            = "# "
	CommandIdentifier = "\\"
	EndIdentifier     = ";"
)

func main() {
	lastInput := ""
	scanner := bufio.NewScanner(os.Stdin)
LOOP:
	for {
		fmt.Print(Prompt)
		// Scans a line from Stdin(Console)
		scanner.Scan()
		// Holds the string that scanned
		text := strings.ToLower(scanner.Text())
		if lastInput != "" {
			text = lastInput + text
			lastInput = ""
		}

		if strings.HasPrefix(text, CommandIdentifier) {
			// Stand for command, like \q, \h, \quit
			comand := strings.Replace(text, CommandIdentifier, "", 1)
			comand = strings.TrimSpace(comand)
			switch parser.ParseCommand(comand) {
			case parser.QuitCommand:
				fmt.Println("quit")
				break LOOP
			case parser.HelpCommand:
				fmt.Println("help")
			case parser.UnknownCommand:
				fmt.Printf("you input an invalid command: [%s]\n", comand)
			}
		} else if strings.HasSuffix(text, EndIdentifier) {
			// Stand for a normal query statement
			source := strings.Replace(text, EndIdentifier, "", 1)
			createStmt, err := lexer.Lex(source)
			if err != nil {
				fmt.Printf("Error: invalid statement %s, error: %s\n", source, err)
				continue LOOP
			}
			fmt.Printf("Create Table: %s ok!\n", createStmt.Name.Value)
		} else {
			// When an unexpected \n or \r is coming, holds it for next loop
			lastInput = text + " "
		}
	}
}
