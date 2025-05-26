package tinydb

//functions to write data to disk

import (
	"fmt"
	"math"
	"math/rand/v2"
	"os"
)

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
	fp, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0x666)
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

// merge logs into data level
func mergeLogs(logs []dblog, data []kvpair) ([]kvpair, error) {
	for _, l := range logs {
		//apply logs actions to row, keeping data sorted
		i := sortIdx(data, l.kvpair)
		switch {
		case l.action == SET_KEY:
			l.csum = csum(l.kvpair) //recompute without action
			//if bigger than whole array, add to end
			if len(data) == i {
				data = append(data, l.kvpair)
				continue
			}
			//otherwise if key exists, just change value
			if data[i].key == l.key {
				data[i] = l.kvpair
				continue
			}
			//otherwise, insert new element into array
			data = append(data, kvpair{}) //grow by 1 element
			copy(data[i+1:], data[i:])    //shift elements right
			data[i] = l.kvpair

		case l.action == DELETE_KEY:
			//key not found case
			if len(data) == i || data[i].key != l.key {
				continue
			}
			//shift next elements left
			if len(data) != i+1 {
				copy(data[i:], data[i+1:])
			}
			//shorten array
			data = data[:len(data)-1]
		default:
			return nil, fmt.Errorf("no matching operation for key, %v", l)
		}
	}
	return data, nil
}

// index where new item should be inserted in sorted kvpair list
func sortIdx(data []kvpair, item kvpair) int {
	for i := range data {
		if data[i].key < item.key {
			continue
		}
		return i
	}
	return len(data)
}

// write vkpairs to disk
func writeKvPairs(file string, data []kvpair) error {
	var s string
	for _, d := range data {
		s += fmt.Sprintln(d) //string "+=" is very slow, reallocates the whole string. This is bad.
	}
	return saveData2(file, []byte(s))
}

// write logs to disk (append)
func appendLogs(file string, data []dblog) error {
	var s string
	for _, d := range data {
		s += fmt.Sprintln(d)
	}
	return appendData(file, []byte(s))
}
