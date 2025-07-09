package tinydb

import (
	"fmt"
	"testing"
)

func trivialTree() *BPlusTree {
	data := []dblog{dblog{key: "mykey", val: "myval"}}
	rootchl := []bptNode{bptNode{test: "a"}, bptNode{test: "b"}, bptNode{test: "c", data: data}}
	r := bptNode{"root", rootchl, nil}
	return &BPlusTree{4, r, 10}
}

func TestWrite1(t *testing.T) {
	tree := trivialTree()
	f := func(q *bptNode, bend, exit func()) {
		fmt.Println(q)
	}

	tree.walk(f)
	log := dblog{key: "b", val: "newval"}
	tree.Write1(log)
	tree.walk(f)
}
