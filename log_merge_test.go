package tinydb

import (
	"fmt"
	"os"
	"runtime/pprof"
	"testing"
	"time"
)

func TestQuickSort(t *testing.T) {
	table := []struct {
		key    string
		val    string
		csum   byte
		action byte
	}{{"key6", ".", 0, DELETE_KEY},
		{"key2", "APPLESAUCE", 0, SET_KEY},
		{"key3", "ICEBERG", 0, SET_KEY},
		{"key9", "ILLITERATE", 0, SET_KEY},
		{"key8", "CRAYON", 0, SET_KEY},
		{"key0", "ARTHUR", 0, SET_KEY},
	}
	logs := make([]dblog, len(table))
	for i, r := range table {
		logs[i] = dblog{key: r.key, val: r.val, action: r.action}
		logs[i].csum = csum(logs[i])
	}
	printLogs(quickSort(logs))
}

func TestLmergeLogs(t *testing.T) {
	// create a few example logs
	otable := []struct {
		key    string
		val    string
		csum   byte
		action byte
	}{{"key0", "ARTHUR", 0, SET_KEY},
		{"key6", ".", 0, DELETE_KEY},
		{"key8", "CRAYON", 0, SET_KEY},
		{"key9", "APPLESAUCE", 0, SET_KEY},
	}
	ntable := []struct {
		key    string
		val    string
		csum   byte
		action byte
	}{{"key1", "ATOM", 0, SET_KEY},
		{"key4", ".", 0, DELETE_KEY},
		{"key7", "CRAYON", 0, SET_KEY},
		{"key99", "APPLESAUCE", 0, SET_KEY},
	}

	ologs := make([]dblog, len(otable))
	for i, r := range otable {
		ologs[i] = dblog{key: r.key, val: r.val, action: r.action}
		ologs[i].csum = csum(ologs[i])
	}
	nlogs := make([]dblog, len(ntable))
	for i, r := range ntable {
		nlogs[i] = dblog{key: r.key, val: r.val, action: r.action}
		nlogs[i].csum = csum(nlogs[i])
	}
	ologs = sortLogs(ologs)
	nlogs = sortLogs(nlogs)
	fmt.Printf("ologs:\n")
	printLogs(ologs)
	fmt.Printf("nlogs:\n")
	printLogs(nlogs)
	merged := lmergeLogs(nlogs, ologs)
	fmt.Printf("merged:\n")
	printLogs(merged)
}

const INGEST_COUNT = 1000000
const GROUP_SIZE = 100000

func TestIngestLogs(t *testing.T) {
	f, _ := os.Create("cpuprof")
	pprof.StartCPUProfile(f)
	resetFiles()
	//generate a bunch of example logs
	db := newLogDB(GROUP_SIZE)
	var i int
	st := time.Now()
	for i < INGEST_COUNT {
		var logs []dblog
		for range GROUP_SIZE {
			l := dblog{key: fmt.Sprintf("key%v", -i), val: "X", action: SET_KEY}
			logs = append(logs, l)
			i++
		}
		err := db.ingestLogs(logs)
		if err != nil {
			fmt.Println(err)
			t.Fail()
		}
	}
	dur := time.Since(st)
	fmt.Printf("total time: %d ms\n", dur/time.Millisecond)
	pprof.StopCPUProfile()
	//printLogs(logs)
	//fmt.Println(db)
}

// creates all file levels, blank
func resetFiles() {
	os.Create("latest.temp")
	os.Create("l1.dbl")
	os.Create("l2.dbl")
	os.Create("l3.dbl")
	os.Create("real.db")
}
