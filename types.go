package main

type kvpair struct {
	key  string
	val  string
	csum uint16 //checksum
}

const (
	DELETE_KEY = iota
	SET_KEY
)

type dblog struct {
	kvpair
	action int
}
