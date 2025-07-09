package tinydb

import "sync"

//implement trivial LogLevel interface for logArray and unsortedLogArray types

type logArray struct {
	name string
	logs []dblog
	mu   sync.Mutex
	lim  int
}

func (a *logArray) Write(logs []dblog) error {
	a.logs = lmergeLogs(logs, a.logs)
	return nil
}

func (a *logArray) Clear() error {
	a.logs = []dblog{}
	return nil
}

func (a *logArray) Save() error {
	data := logsToBytes(a.logs)
	return saveData2(a.name, data)
}

func (a *logArray) Size() int {
	return len(a.logs)
}

func (a *logArray) Lock() {
	a.mu.Lock()
}

func (a *logArray) Unlock() {
	a.mu.Unlock()
}

func (a *logArray) Limit() int {
	return a.Limit()
}

type unsortedLogArray logArray

// only difference in methods for unsorted vs sorted implementations.
func (u *unsortedLogArray) Write(logs []dblog) error {
	u.logs = append(u.logs, logs...)
	return nil
}

func (u *unsortedLogArray) Clear() error {
	return (*logArray)(u).Clear()
}
func (u *unsortedLogArray) Save() error {
	return (*logArray)(u).Save()
}

func (u *unsortedLogArray) Size() error {
	return (*logArray)(u).Clear()
}
func (u *unsortedLogArray) Lock() {
	(*logArray)(u).Lock()
}
func (u *unsortedLogArray) Unlock() {
	(*logArray)(u).Unlock()
}
func (u *unsortedLogArray) Limit() int {
	return (*logArray)(u).Limit()
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
