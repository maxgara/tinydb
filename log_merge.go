package tinydb

import (
	"fmt"
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
const LEVEL_FILE_MAXSIZE = 100000 //split level files up after this

type LogDB struct {
	levels  []string //level file. L0 is latest.temp
	l_locks []sync.Mutex
}

// process new log batch. safe for parallel
func (db LogDB) ingestLogs(logs []dblog) {
	db.l_locks[0].Lock()
	appendLogs(db.levels[0], logs)
	//TODO: level merge up
	db.l_locks[0].Unlock()
}

// like lmergeLogs but for files. safe for parallel.
func lfmergeLogs(dlevel, slevel int, db LogDB) {
	db.l_locks[dlevel].Lock()
	db.l_locks[slevel].Lock()
	//TODO: parse logs
	var slogs, dlogs []dblog
	dlogs = lmergeLogs(dlogs, slogs)
	saveData2(db.levels[dlevel], []byte(fmt.Sprint(dlogs))) //TODO: string rep improvement
	db.l_locks[dlevel].Unlock()
	db.l_locks[slevel].Unlock()
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
