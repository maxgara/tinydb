package tinydb

import "sync"

//implement trivial LogLevel interface for logArray and unsortedLogArray types

type logArray struct {
	name string
	logs []dblog
	mu   sync.Mutex
}

func (a *logArray) write(logs []dblog) error {
	a.logs = lmergeLogs(logs, a.logs)
	return nil
}

func (a *logArray) clear() error {
	a.logs = []dblog{}
	return nil
}

func (a *logArray) save() error {
	data := logsToBytes(a.logs)
	return saveData2(a.name, data)
}

func (a *logArray) size() int {
	return len(a.logs)
}

func (a *logArray) lock() {
	a.mu.Lock()
}

func (a *logArray) unlock() {
	a.mu.Unlock()
}

type unsortedLogArray logArray

// only difference in methods for unsorted vs sorted implementations.
func (u *unsortedLogArray) write(logs []dblog) error {
	u.logs = append(u.logs, logs...)
	return nil
}

func (u *unsortedLogArray) clear() error {
	return (*logArray)(u).clear()
}
func (u *unsortedLogArray) save() error {
	return (*logArray)(u).save()
}

func (u *unsortedLogArray) size() error {
	return (*logArray)(u).clear()
}
func (u *unsortedLogArray) lock() {
	(*logArray)(u).lock()
}
func (u *unsortedLogArray) unlock() {
	(*logArray)(u).unlock()
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
