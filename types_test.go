package tinydb

import (
	"testing"
)

// create a few example kvpairs
func genlogs1() []dblog {
	data := []dblog{{"key1", "abe", 0, 0},
		{"key2", "bob", 0, 0},
		{"key3", "cathy", 0, 0},
		{"key4", "domion", 0, 0},
		{"key5", "ester", 0, 0}}
	for i := range data {
		data[i].csum = csum(data[i])
	}
	return data
}

// create a few example logs
func genlogs2() []dblog {
	table := []dblog{{"key1", "ARTHUR", 0, SET_KEY},
		{"key2", ".", 0, DELETE_KEY},
		{"key3", "CRAYON", 0, SET_KEY},
		{"key6", "APPLESAUCE", 0, SET_KEY},
		{"key2", "bobblehead", 0, SET_KEY}}
	logs := make([]dblog, len(table))
	for i, r := range table {
		logs[i] = r
		logs[i].csum = csum(logs[i])
	}
	return logs
}
func TestPrintLogs(t *testing.T) {
	logs := genlogs1()
	printLogs(logs)
}
