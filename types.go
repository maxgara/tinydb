package tinydb

import "fmt"

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
	action int
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
	return fmt.Sprintf("%x\t%v=%v", p.csum, p.key, p.val)
}

func (p dblog) String() string {
	var a string
	switch p.action {
	case DELETE_KEY:
		a = "DEL"
	case SET_KEY:
		a = "SET"
	}
	return fmt.Sprintf("%v %v=%v", a, p.key, p.val)
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
