package tinydb

//definitions of types used in tinydb implementation.

import (
	"fmt"
	"sync"
)

type LogDB struct {
	levels  []string //level file. L0 is latest.temp
	lsizes  []int    //count of logs/kvpairs in each level
	l_locks []sync.Mutex
	l_lims  []int //max size in each level before merge is triggered
}

// will be implemented by storage structures: unsorted_LogArray, logArray, BPTree, etc.
// represents a level of log structure merge tree
// one major operation not implemented at Interface is merge; this cannot be done per implementation of logLevel,
// has to be done per PAIR of implementations as merging a logArray into a logArray is different than merging a BPTree, etc.
// naturally some merge operand combinations will not be supported.
type logLevel interface {
	write(logs []dblog) error //write logs
	clear() error             //delete all logs
	save() error              //save to file
	size() int                //current log count
	lock()                    //mutex Lock
	unlock()                  //mutex Unlock
}

// key value pair. Important: caller is responsible for setting the csum value.
type kvpair struct {
	key  string
	val  string
	csum byte //checksum
}

const (
	DELETE_KEY = iota
	SET_KEY
)

// database log, extends kvpair. actions defined above (currently set or delete)
type dblog struct {
	kvpair
	action byte
}

// leaf node of B+ tree.
type bleaf struct {
	test string //if key<test, pick this leaf node
	data []kvpair
}

// branch/root node of B+ tree
type bnode struct {
	branches []bleaf
}

func (p kvpair) String() string {
	return fmt.Sprintf("%x %v=%v", p.csum, p.key, p.val)
}

func (p dblog) String() string {
	var a string
	switch p.action {
	case DELETE_KEY:
		a = "DEL"
	case SET_KEY:
		a = "SET"
	}
	return fmt.Sprintf("%x %v %v=%v", p.csum, a, p.key, p.val)
}
func printData(data []kvpair) {
	for _, l := range data {
		fmt.Println(l)
	}
}
func printLogs(logs []dblog) {
	for _, l := range logs {
		fmt.Println(l)
	}
}

// write byte slice for saving to file
// format is:
// csum 			(1 byte)
// key				(string)
// separator 		(1 byte)
// val 				(string)
// separator 		(1 byte)
// action 			(1 byte)
// newline 			(1 byte)
//
// more efficient than writing strings because strings get copied every time they are assigned
func logsToBytes(logs []dblog) []byte {
	const sep = byte(',')
	var b []byte
	for _, l := range logs {
		b = append(b, byte(l.csum)) //careful when parsing, will have csum == sep sometimes.
		b = append(b, []byte(l.key)...)
		b = append(b, sep)
		b = append(b, []byte(l.val)...)
		b = append(b, sep)
		b = append(b, byte(l.action))
		b = append(b, '\n')
	}
	return b
}

// parse log data into log objects
func parseLogs(f []byte) ([]dblog, error) {
	var logs []dblog
	var log dblog
	var i int
	//loop over lines:
	for i < len(f) {
		//get csum
		log.csum = f[i]
		//get key
		i++
		marker := i
		for i < len(f) && f[i] != ',' {
			i++
		}
		log.key = string(f[marker:i])
		//get val
		i++ //skip delimiter
		marker = i
		for i < len(f) && f[i] != ',' {
			i++
		}
		log.val = string(f[marker:i])
		//get action
		i++ //skip delimiter
		if i >= len(f) {
			return nil, fmt.Errorf("parseLogs: unexpected EOF, expected action")
		}
		log.action = f[i]
		i += 2 //move to next line
		logs = append(logs, log)
	}
	return logs, nil
}

// xor based checksum of kvpair
func csum(p kvpair) byte {
	s := p.key + p.val
	var ch byte = 0x0
	for i := range s {
		ch = ch ^ s[i]
	}
	return ch
}

// xor based checksum for dblog
func csumLog(l dblog) byte {
	return csum(l.kvpair) ^ byte(l.action)
}
