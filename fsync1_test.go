package tinydb

import (
	"fmt"
	"testing"
)

func Test_writeLogs(t *testing.T) {
	logs := genlogs2()
	err := appendLogs("test", logs)
	if err != nil {
		t.Fail()
		fmt.Println(err)
	}
}
