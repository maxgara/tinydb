package tinydb

import (
	"fmt"
	"testing"
)

// create a few example kvpairs
func gendata() []kvpair {
	data := []kvpair{{"key1", "abe", 0},
		{"key2", "bob", 0},
		{"key3", "cathy", 0},
		{"key4", "domion", 0},
		{"key5", "ester", 0}}
	for i := range data {
		data[i].csum = csum(data[i])
	}
	return data
}

// create a few example logs
func genlogs() []dblog {
	//horrible but effective
	table := []struct {
		key    string
		val    string
		csum   byte
		action int
	}{{"key1", "ARTHUR", 0, SET_KEY},
		{"key2", ".", 0, DELETE_KEY},
		{"key3", "CRAYON", 0, SET_KEY},
		{"key6", "APPLESAUCE", 0, SET_KEY},
		{"key2", "bobblehead", 0, SET_KEY}}

	logs := make([]dblog, len(table))
	for i, r := range table {
		logs[i] = dblog{kvpair: kvpair{key: r.key, val: r.val}, action: r.action}
		logs[i].csum = csumLog(logs[i])
	}
	return logs
}
func TestPrintData(t *testing.T) {
	data := gendata()
	fmt.Println(data)
}
