package tinydb

import (
	"fmt"
)

//implement dblogLevel interface for BPlusTree

//write(logs []dblog) error //write logs
//clear() error             //delete all logs
//save() error              //save to file
//size() int                //current log count
//limit() int               //when to merge up
//lock()                    //mutex Lock
//unlock()                  //mutex Unlock

// B+ Tree
type BPlusTree struct {
	size  int // what/why?
	root  bptNode
	limit int // what is this for??
}

// bptNode of B+ tree
// only one structure is declared, which will be sufficient for both leaf and non-leaf bptNode types.
// this allows all nodes to go in the same array without an extra layer of pointers.
// if leaf bptNode, branches will be empty.
// if non-leaf bptNode, data will be empty
type bptNode struct {
	test     string //if key<test, pick this node
	branches []bptNode
	data     []dblog
}

// write each log into the first B+ tree leaf node with test geq. to key
func (t *BPlusTree) Write(logs []dblog) error {
	//this is the complicated one
	if len(logs) == 0 {
		return fmt.Errorf("Write: 0 len logs array error\n")
	}
	f := func(q *bptNode, bend, exit func()) {
		low := logs[0].key
		if low > q.test {
			bend()
		}
		//if on leaf node, write some of the logs
		if q.branches == nil {
			stopidx := -1
			for i := range logs {
				if logs[i].key > q.test {
					stopidx = i
					break
				}
			}
			if stopidx == -1 {
				q.data = lmergeLogs(logs, q.data)
				exit()
				return
			}
			q.data = lmergeLogs(logs[:stopidx], q.data)
			logs = logs[stopidx:]
		}
	}
	t.walk(f)
	return nil
}

// write 1 log
func (t *BPlusTree) Write1(log dblog) {
	//fmt.Printf("Write1 for log %v\n", log)
	var found bool
	f := func(q *bptNode, bend, exit func()) {
		if log.key <= q.test {
			//fmt.Printf("got key<test - picking node {key=%v, test=%v, node=%p}\n", log.key, q.test, &q)
			if q.branches == nil {
				q.data = append(q.data, log)
				found = true
				//fmt.Printf("got data leaf node - exiting at %p [%v]", &q, q.data)
				exit()
			}
			return
		}
		//fmt.Printf("key>test - bend(%p)", &q)
		bend() // do not look into this branch
	}
	t.walk(f)
	if !found {
		fmt.Println("Write1: non-written error")
	}
}

// walk tree, calling f on each node. if f calls bend stop recursive exec. on child branches of current node.
// if f calls exit walk returns
func (t *BPlusTree) walk(f func(q *bptNode, bend func(), exit func())) {
	exit := func() {
		//fmt.Println("walk: end walk (top level exit called)")
	}
	rwalk(&t.root, f, exit)
	//fmt.Println("walk complete")
}

// recursive walk
// f can call 2 functions:
// exit() stops walk immediatelly
// bend() ends current branch but does not stop walk,
func rwalk(r *bptNode, f func(q *bptNode, bend func(), exit func()), pexit func()) {
	var stop bool
	// branch end mechanism
	bend := func() {
		fmt.Printf("bend called")
		stop = true
	}
	exit := func() {
		fmt.Printf("exit called")
		stop = true
		pexit()
	}
	f(r, bend, exit)
	if stop {
		return
	}
	//if recurstion not stopped by f, call f on each child node
	for i := range r.branches {
		rwalk(&r.branches[i], f, exit)
		if stop {
			break
		}
	}
}

// delete all logs
func (t *BPlusTree) Clear() error {
	t.root = bptNode{}
	t.size = 0
	return nil
}

//func (t *BPlusTree) Save() error //save to file
//func (t *BPlusTree) Size() int   //current log count
//func (t *BPlusTree) Limit() int  //when to merge up
//func (t *BPlusTree) Lock()       //mutex Lock
//func (t *BPlusTree) Unlock()     //mutex Unlock

// call f on each leaf node l of t
func (t *BPlusTree) EachLeaf(f func(bptNode)) {

}
func nEachLeaf(q bptNode, f func(bptNode)) {

}
