package raft

import (
	"course/labgob"
	"fmt"
)

type RaftLog struct {
	snapLastIndex int
	snapLastTerm  int

	// contain log[:snapLastIndex]
	snapshot []byte

	// contain log[snapLastIndex:]
	tailLog []LogEntry
}

func NewLog(snapLastIndex, snapLastTerm int, snapshot []byte, entries []LogEntry) *RaftLog {
	rl := &RaftLog{
		snapLastIndex: snapLastIndex,
		snapLastTerm:  snapLastTerm,
		snapshot:      snapshot,
	}
	rl.tailLog = append(rl.tailLog, LogEntry{
		Term: snapLastTerm,
	})
	rl.tailLog = append(rl.tailLog, entries...)
	return rl
}

func (rl *RaftLog) readPersist(d *labgob.LabDecoder) error {
	var lastIdx int
	if err := d.Decode(&lastIdx); err != nil {
		return fmt.Errorf("decode last include index failed")
	}
	rl.snapLastIndex = lastIdx

	var lastTerm int
	if err := d.Decode(&lastTerm); err != nil {
		return fmt.Errorf("decode last include term failed")
	}
	rl.snapLastTerm = lastTerm

	var log []LogEntry
	if err := d.Decode(&log); err != nil {
		return fmt.Errorf("decode last include term failed")
	}
	rl.tailLog = log

	return nil
}

func (rl *RaftLog) persist(e *labgob.LabEncoder) {
	e.Encode(rl.snapLastIndex)
	e.Encode(rl.snapLastTerm)
	e.Encode(rl.tailLog)
}

func (rl *RaftLog) size() int {
	return len(rl.tailLog) + rl.snapLastIndex
}

// index convertion
func (rl *RaftLog) idx(logicIndex int) int {
	if logicIndex < rl.snapLastIndex || logicIndex >= rl.size() {
		panic(fmt.Sprintf("%d is out of index range [%d %d]", logicIndex, rl.snapLastIndex, rl.size()-1))
	}
	return logicIndex - rl.snapLastIndex
}

func (rl *RaftLog) at(logicIndex int) LogEntry {
	return rl.tailLog[rl.idx(logicIndex)]
}

func (rl *RaftLog) firstLogFor(term int) int {
	for idx, entry := range rl.tailLog {
		if entry.Term == term {
			return idx + rl.snapLastIndex
		} else if entry.Term > term {
			break
		}
	}
	return InvalidIndex
}

func (rl *RaftLog) doSnapshot(index int, snapshot []byte) {
	if index <= rl.snapLastIndex {
		return
	}
	idx := rl.idx(index)
	rl.snapshot = snapshot
	rl.snapLastIndex = index
	rl.snapLastTerm = rl.tailLog[idx].Term

	newLog := make([]LogEntry, 0, rl.size()-rl.snapLastIndex)
	newLog = append(newLog, LogEntry{
		Term: rl.snapLastTerm,
	})
	newLog = append(newLog, rl.tailLog[idx+1:]...)
	rl.tailLog = newLog
}

func (rl *RaftLog) last() (index, term int) {
	i := len(rl.tailLog) - 1
	return rl.snapLastIndex + i, rl.tailLog[i].Term
}

func (rl *RaftLog) append(logs ...LogEntry) {
	rl.tailLog = append(rl.tailLog, logs...)
}

func (rl *RaftLog) appendFrom(logicIndex int, logs ...LogEntry) {
	index := rl.idx(logicIndex)
	rl.tailLog = append(rl.tailLog[:index+1], logs...)
}

func (rl *RaftLog) tail(index int) []LogEntry {
	if index >= rl.size() {
		return nil
	}
	return rl.tailLog[rl.idx(index):]
}

func (rl *RaftLog) installSnapshot(index int, term int, snapshot []byte) {
	rl.snapshot = snapshot
	rl.snapLastIndex = index
	rl.snapLastTerm = term

	newLog := make([]LogEntry, 0, 1)
	newLog = append(newLog, LogEntry{
		Term: rl.snapLastTerm,
	})
	rl.tailLog = newLog
}
