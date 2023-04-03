package storage

import (
	"fmt"
)

// All created tables will be stored in this map
var tables = make(map[string]Table)

// The global configrations
var config Config

func init() {
	c, err := readConfig()
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	if c == nil {
		// run in test mode
		return
	}
	config = *c

	// First, loads all schemes to restore tables
	loadSchemes()
	// Second, loads binary data to restore rows
	loadRows()
}
