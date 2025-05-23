package main

//functions to write data to disk

import (
	"fmt"
	"math"
	"math/rand/v2"
	"os"
)

func main() {
	saveData2("./datafile.txt", []byte("this is test data"))
}

// atomic write - new file
func saveData2(path string, data []byte) error {
	tmp := fmt.Sprintf("%v.tmp.%d", path, randomInt())
	fp, err := os.Create(tmp)
	if err != nil {
		return err
	}
	defer func() {
		fp.Close()
		if err != nil {
			os.Remove(tmp)
		}
	}()
	if _, err = fp.Write(data); err != nil {
		return err
	}
	if err = fp.Sync(); err != nil { //fsync
		return err
	}
	err = os.Rename(tmp, path)
	return err
}

// append data - return error if not completed
func appendData(path string, data []byte) error {
	fp, err := os.OpenFile(path, os.O_APPEND, os.ModeAppend.Perm())
	if err != nil {
		return err
	}
	defer func() {
		fp.Close()
	}()
	if _, err = fp.Write(data); err != nil {
		return err
	}
	err = fp.Sync()
	return err
}

func randomInt() uint {
	return rand.UintN(math.MaxUint)
}

// merge logs into database file
func mergeLogs(logs []dblog, data []kvpair) error {
	for _, l := range logs {
		i := sortIdx(data, l.kvpair)
		if data[i].key == l.key && l.action == DELETE_KEY {
			if len(data) != i+1 {
				copy(data[i:], data[i+1:])
			}
			data = data[:len(data)-1]
		}
		if data[i].key == l.key && l.action == SET_KEY {
			data[i] = l.kvpair
		}
	}
	return nil
}

// index where new item should be inserted in sorted kvpair list
func sortIdx(data []kvpair, item kvpair) int {
	for i := range data {
		if data[i].key < item.key {
			continue
		}
		return i
	}
	return len(data) + 1
}
