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

const BASE_LEVEL_SIZE = 4
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
			fmt.Printf("over cap on %v, merging to %v", i, i+1)
			err = lfmergeLogs(i+1, i, db)
			if err != nil {
				return err
			}
			err = saveData2(db.levels[i], []byte{}) //wipe file
			db.lsizes[i] = 0
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
	balance(db)
	return err
	//TODO: level merge up
}

// like lmergeLogs but for files. safe for parallel.
func lfmergeLogs(dlevel, slevel int, db LogDB) error {
	db.l_locks[dlevel].Lock()
	db.l_locks[slevel].Lock()
	defer func() {
		db.l_locks[dlevel].Unlock()
		db.l_locks[slevel].Unlock()
	}()
	//TODO: parse logs
	sstr, err := os.ReadFile(db.levels[slevel])
	if err != nil {
		return err
	}
	dstr, err := os.ReadFile(db.levels[dlevel])
	if err != nil {
		return err
	}
	slogs := parseLogs(string(sstr))
	dlogs := parseLogs(string(dstr))
	dlogs = lmergeLogs(dlogs, slogs)
	err = saveData2(db.levels[dlevel], []byte(fmt.Sprint(dlogs))) //TODO: string rep improvement
	return err
}

// parse log data into log objects
func parseLogs(f string) []dblog {
	//TODO
}

// O(n) merge logs into other logs. both sets must be pre-sorted.
func lmergeLogs(nlogs, ologs []dblog) (final []dblog) {
	final = make([]dblog, len(nlogs)+len(ologs))
	var omark int //olog idx
	//interleave ologs and nlogs
	for i, l := range nlogs {
		//final contains mark + i elements
		count := i + omark
		for {
			// no more ologs
			if omark == len(ologs) {
				final[count] = l
				break
			}
			// insert nlog
			if ologs[omark].key >= l.key {
				final[count] = l
				break // next nlog
			}
			// insert olog
			final[count] = ologs[omark]
			omark++
			count++
		}
	}
	// no more ilogs
	for omark < len(ologs) {
		ologs[len(nlogs)+omark] = ologs[omark]
		omark++
	}
	return
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
