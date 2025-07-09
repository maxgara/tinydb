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

// TODO: implement test
func Test_bptreeWrite(t *testing.T) {
	tree := trivialTree()
	f := func(q *bptNode, bend, exit func()) {
		fmt.Println(q)
	}
	tree.walk(f)
	logs := []dblog{{key: "b", val: "newval1"}, dblog{key: "b", val: "newval2"}}
	tree.Write(logs)
	tree.walk(f)
	//needs more complex cases
}
