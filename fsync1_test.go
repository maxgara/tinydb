package tinydb

import (
	"fmt"
	"testing"
)

func Test_writeKvPairs(t *testing.T) {
	data := gendata()
	err := writeKvPairs("test", data)
	if err != nil {
		t.Fail()
	}
}

func Test_writeLogs(t *testing.T) {
	logs := genlogs()
	err := appendLogs("test", logs)
	if err != nil {
		t.Fail()
		fmt.Println(err)
	}
}

func Test_mergeLogs(t *testing.T) {
	data := gendata()
	logs := genlogs()
	data, err := mergeLogs(logs, data)
	if err != nil {
		t.Fail()
		fmt.Println(err)
	}
	fmt.Println(data)
}
