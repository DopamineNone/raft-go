package raft

import (
	"bytes"
	"course/labgob"
	"fmt"
)

func (rf *Raft) persistString() string {
	return fmt.Sprintf("T%d, VotedFor: %d, Log: [0: %d)", rf.currentTerm, rf.voteFor, rf.log.size()-1)
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persistLocked() {
	// Your code here (PartC).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	rf.log.persist(e)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.log.snapshot)
	LOG(rf.me, rf.currentTerm, DPersist, "Persist %s", rf.persistString())
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (PartC).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }

	var (
		currentTerm int
		voteFor     int
	)

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	if err := d.Decode(&currentTerm); err != nil {
		LOG(rf.me, rf.currentTerm, DPersist, "Decode currentTerm failed: %v", err)
		return
	}
	rf.currentTerm = currentTerm
	if err := d.Decode(&voteFor); err != nil {
		LOG(rf.me, rf.currentTerm, DPersist, "Decode voteFor failed: %v", err)
		return
	}
	rf.voteFor = voteFor
	if err := rf.log.readPersist(d); err != nil {
		LOG(rf.me, rf.currentTerm, DPersist, "Decode log failed: %v", err)
		return
	}
	rf.log.snapshot = rf.persister.ReadSnapshot()

	if rf.log.snapLastIndex > rf.commitIndex {
		rf.commitIndex = rf.log.snapLastIndex
		rf.lastApplied = rf.log.snapLastIndex
	}
	LOG(rf.me, rf.currentTerm, DPersist, "Restore from %s", rf.persistString())
}
