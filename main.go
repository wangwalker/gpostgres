package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/wangwalker/gpostgres/internal/parser"
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
			fmt.Printf("you input a SQL statement: %s\n", text)
		} else {
			// When an unexpected \n or \r is coming, holds it for next loop
			lastInput = text + " "
		}
	}
}
