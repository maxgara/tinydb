package tinydb

import (
	"fmt"
	"os"
	"sync"
)

// merge logs by levels, using simple sorted array for each level
// logs are received in unordered blocks, saved into latest.temp
// logs in latest.temp are sorted into l1.dbl. as this is done the sorted logs are removed from latest.temp
// logs in l1 are merged into l2, then l3 as soon as each log file hits a prerequisite size (merge threshold)
// each level has a merge threshold 2x the previous level (so if l3 has a threshold of 1000 logs, l2 has 2000 etc.)
// when l3 hits threshold, it is sorted into real.db, which holds kvpairs instead of logs (pretty arbitrary distinction though)

const BASE_LEVEL_SIZE = 10
const LEVEL_COUNT = 4

type LogDB struct {
	levels  []string //level file. L0 is latest.temp
	lsizes  []int    //count of logs/kvpairs in each level
	l_locks []sync.Mutex
}

func newLogDB() LogDB {
	db := LogDB{}
	db.levels = []string{"latest.temp"}
	db.l_locks = make([]sync.Mutex, LEVEL_COUNT)
	db.lsizes = make([]int, LEVEL_COUNT)

	for i := range LEVEL_COUNT - 1 {
		db.levels = append(db.levels, fmt.Sprintf("l%d.dbl", i+1))
	}
	return db
}

// if any level is over cap, merge up. Uses lfmergeLogs
func balance(db LogDB) error {
	max := BASE_LEVEL_SIZE
	var err error
	for i, sz := range db.lsizes[:len(db.levels)-1] {
		if sz > max {
			//fmt.Printf("over cap on %v, merging to %v\n", i, i+1)
			//st := time.Now()
			err = lfmergeLogs(i+1, i, db)
			if err != nil {
				return err
			}
			err = saveData2(db.levels[i], []byte{}) //wipe file
			db.lsizes[i+1] += db.lsizes[i]
			db.lsizes[i] = 0
			//dur := time.Since(st)
			//fmt.Printf("merged in %d ms\n", dur/time.Millisecond)
		}
		max *= 2
	}
	return err
}

// process new log batch. safe for parallel
func (db LogDB) ingestLogs(logs []dblog) error {
	db.l_locks[0].Lock()
	err := appendLogs(db.levels[0], logs)
	if err != nil {
		db.l_locks[0].Unlock()
		return err
	}
	fmt.Println("appended logs to db level 0")
	db.lsizes[0] += len(logs)
	db.l_locks[0].Unlock()
	err = balance(db)
	return err
}

// like lmergeLogs but for files. safe for parallel.
func lfmergeLogs(dlevel, slevel int, db LogDB) error {
	db.l_locks[dlevel].Lock()
	db.l_locks[slevel].Lock()
	defer func() {
		db.l_locks[dlevel].Unlock()
		db.l_locks[slevel].Unlock()
	}()
	sstr, err := os.ReadFile(db.levels[slevel])
	if err != nil {
		return err
	}
	dstr, err := os.ReadFile(db.levels[dlevel])
	if err != nil {
		return err
	}
	slogs, err := parseLogs(sstr)
	if err != nil {
		return err
	}
	dlogs, err := parseLogs(dstr)
	if err != nil {
		return err
	}
	//must sort logs in latest.temp before merging
	if slevel == 0 {
		fmt.Println("flmerge: sorting level 0")
		slogs = quickSort(slogs)
	}
	dlogs = lmergeLogs(dlogs, slogs)
	err = saveData2(db.levels[dlevel], logsToBytes(dlogs)) //TODO: string rep improvement
	return err
}

// O(n) merge logs into other logs. both sets must be pre-sorted.
func lmergeLogs(nlogs, ologs []dblog) (sorted []dblog) {
	size := len(nlogs) + len(ologs)
	sorted = make([]dblog, size)
	var omark int //olog idx
	var nmark int //nlog idx
	var i int     //sorted idx
	//interleave ologs and nlogs, picking the smallest option each time
	for i < size {
		switch {
		// no more ologs
		case omark == len(ologs):
			sorted[i] = nlogs[nmark]
			nmark++
		// no more nlogs
		case nmark == len(nlogs):
			sorted[i] = ologs[omark]
			omark++
		//insert olog
		case ologs[omark].key <= nlogs[nmark].key:
			sorted[i] = ologs[omark]
			omark++
		//insert nlog
		default:
			sorted[i] = nlogs[nmark]
			nmark++
		}
		i++
	}
	return
}

func quickSort(logs []dblog) []dblog {
	if len(logs) == 2 {
		if logs[0].key >= logs[1].key {
			logs[0].key, logs[1].key = logs[1].key, logs[0].key
		}
		return logs
	}
	if len(logs) == 1 {
		return logs
	}
	//recursive case
	i := len(logs) / 2
	s1 := logs[:i]
	s2 := logs[i:]
	s1 = quickSort(s1)
	s2 = quickSort(s2)
	qsmerge(s1, s2, logs)
	return logs
}
func qsmerge(iset, jset, out []dblog) {
	var i, j int
	temp := make([]dblog, len(iset)+len(jset))
	for idx := range len(iset) + len(jset) {
		switch {
		case i >= len(iset):
			temp[idx] = jset[j]
			j++
		case j >= len(jset):
			temp[idx] = iset[i]
			i++
		case iset[i].key < jset[j].key:
			temp[idx] = iset[i]
			i++
		default:
			temp[idx] = jset[j]
			j++
		}
	}
	copy(out, temp) //allows memory conservation
}

// trivial sorting algorithm
func sortLogs(logs []dblog) (sorted []dblog) {
	sorted = make([]dblog, len(logs))
	for i, l := range logs {
		idx := basicSortidx(logs, l)
		if idx == -1 {
			sorted[i] = l //put at end; log keeps original position
		}
		//sorted array has current used size of i-1. new used size will be i.
		copy(sorted[idx+1:i+1], sorted[idx:i]) //shift array to the right to make room for l. only move elements actually filled.
		sorted[idx] = l
	}
	return
}

// get sort index, linear sorting algo. returns index where l should be placed in existing array, or -1 if it goes at the end
func basicSortidx(logs []dblog, l dblog) int {
	for i := range logs {
		if logs[i].key < l.key {
			continue
		}
		return i
	}
	return -1
}
