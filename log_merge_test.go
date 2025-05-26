package tinydb

import (
	"fmt"
	"testing"
)

func TestLmergeLogs(t *testing.T) {
	// create a few example logs
	otable := []struct {
		key    string
		val    string
		csum   byte
		action int
	}{{"key0", "ARTHUR", 0, SET_KEY},
		{"key6", ".", 0, DELETE_KEY},
		{"key8", "CRAYON", 0, SET_KEY},
		{"key9", "APPLESAUCE", 0, SET_KEY},
	}
	ntable := []struct {
		key    string
		val    string
		csum   byte
		action int
	}{{"key1", "ATOM", 0, SET_KEY},
		{"key4", ".", 0, DELETE_KEY},
		{"key7", "CRAYON", 0, SET_KEY},
		{"key99", "APPLESAUCE", 0, SET_KEY},
	}

	ologs := make([]dblog, len(otable))
	for i, r := range otable {
		ologs[i] = dblog{kvpair: kvpair{key: r.key, val: r.val}, action: r.action}
		ologs[i].csum = csumLog(ologs[i])
	}
	nlogs := make([]dblog, len(ntable))
	for i, r := range ntable {
		nlogs[i] = dblog{kvpair: kvpair{key: r.key, val: r.val}, action: r.action}
		nlogs[i].csum = csumLog(nlogs[i])
	}
	ologs = sortLogs(ologs)
	nlogs = sortLogs(nlogs)
	fmt.Printf("ologs:\n%v\n", ologs)
	fmt.Printf("nlogs:\n%v\n", nlogs)
	fmt.Println(lmergeLogs(nlogs, ologs))

}

func TestIngestLogs(t *testing.T) {
	//generate a bunch of example logs
	var logs []dblog
	db := newLogDB()
	for i := range 1000 {
		l := dblog{kvpair: kvpair{key: fmt.Sprintf("key%v", i), val: "X"}, action: SET_KEY}
		logs = append(logs, l)
	}
	err := db.ingestLogs(logs)
	if err != nil {
		fmt.Println(err)
		t.Fail()
	}
	fmt.Println(db)
}
