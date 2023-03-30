package storage

import (
	"fmt"
)

// All created tables will be stored in this map
var tables = make(map[string]Table)

// The global configrations
var config Config

// for testing from REPL
func init() {
	c, err := readConfig()
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	config = *c

	loadSchemes()
}
