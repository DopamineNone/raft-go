package raft

import (
	"sort"
	"time"
)

type LogEntry struct {
	Term         int
	CommandValid bool        // if it should be applied
	Command      interface{} // the command should be applied to the state machine
}

type AppendEntriesArgs struct {
	Term     int
	LeaderID int

	// log entries
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry

	LeaderCommit int // leader's commit index
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	LOG(rf.me, rf.currentTerm, DLog, "AppendEntries from S%d, T%d", args.LeaderID, args.Term)

	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Reject, higher term, T%d->T%d", args.LeaderID, rf.currentTerm, args.Term)
		return
	}

	if args.Term >= rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	}

	defer func() {
		rf.resetElectionTimeLocked()
		//if !reply.Success {
		//
		//}
	}()

	if args.PrevLogIndex >= rf.log.size() {
		reply.ConflictIndex = rf.log.size()
		reply.ConflictTerm = InvalidTerm
		LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Reject, PrevLogIndex out of bound", args.LeaderID)
		return
	}
	targetTerm := rf.log.at(args.PrevLogIndex).Term
	if args.PrevLogTerm != targetTerm {
		reply.ConflictTerm = targetTerm
		reply.ConflictIndex = rf.log.firstLogFor(reply.ConflictTerm)
		LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Reject, PrevLogTerm mismatch", args.LeaderID)
		return
	}

	// append log entries
	rf.log.appendFrom(args.PrevLogIndex, args.Entries...)
	rf.persistLocked()
	reply.Success = true
	LOG(rf.me, rf.currentTerm, DLog, "from S%d, AppendEntries success", args.LeaderID)

	// handle LeaderCommit
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
		rf.applyCond.Signal()
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// could only replicate in the given term
func (rf *Raft) replicationTicker(term int) {
	for !rf.killed() {
		// send heartbeat
		ok := rf.startReplication(term)
		if !ok {
			break
		}
		// time block
		time.Sleep(replicationInterval)
	}
}

func (rf *Raft) getMajorityMatchedLocked() int {
	tmpIndex := make([]int, len(rf.matchIndex))
	copy(tmpIndex, rf.matchIndex)
	sort.Ints(tmpIndex)
	majority := (len(rf.peers) - 1) / 2
	LOG(rf.me, rf.currentTerm, DLog, "-> Get majority matched: %d", tmpIndex[majority])
	return tmpIndex[majority]
}

func (rf *Raft) startReplication(term int) bool {
	replicateToPeer := func(peer int, args *AppendEntriesArgs) {
		reply := new(AppendEntriesReply)
		ok := rf.sendAppendEntries(peer, args, reply)

		rf.mu.Lock()
		defer rf.mu.Unlock()

		if !ok {
			LOG(rf.me, rf.currentTerm, DLog, "Lost or error, S%d", peer)
			return
		}

		// align the term
		if reply.Term > rf.currentTerm {
			rf.becomeFollowerLocked(reply.Term)
			return
		}

		// check context lost
		if rf.contextLostLocked(Leader, term) {
			LOG(rf.me, rf.currentTerm, DLog, "Lost context, abort AppendEntries for S%d", rf.me)
			return
		}

		// handle reply
		// probe the lower index if the preLog not matched
		if !reply.Success {
			prevIndex := rf.nextIndex[peer]
			if reply.ConflictTerm == InvalidTerm {
				rf.nextIndex[peer] = reply.ConflictIndex
			} else {
				firstLogForTerm := rf.log.firstLogFor(reply.ConflictTerm)
				if firstLogForTerm != InvalidIndex {
					rf.nextIndex[peer] = firstLogForTerm + 1
				} else {
					rf.nextIndex[peer] = reply.ConflictIndex
				}
			}
			// avoid unorder reply
			if rf.nextIndex[peer] > prevIndex {
				rf.nextIndex[peer] = prevIndex
			}
			//rf.nextIndex[peer] = idx + 1
			LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Reject, PrevLogIndex out of bound", peer)
			return
		}

		// update match/next index if success
		rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1
		LOG(rf.me, rf.currentTerm, DLog, "-> S%d, AppendEntries success", peer)

		// update commit
		majorityMatched := rf.getMajorityMatchedLocked()
		if majorityMatched > rf.commitIndex && rf.log.at(majorityMatched).Term == term {
			rf.commitIndex = majorityMatched
			rf.applyCond.Signal()
		}
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.contextLostLocked(Leader, term) {
		LOG(rf.me, rf.currentTerm, DLog, "Lost context, abort AppendEntries for S%d", rf.me)
		return false
	}

	for peer := range rf.peers {
		if peer == rf.me {
			rf.matchIndex[peer] = rf.log.size() - 1
			rf.nextIndex[peer] = rf.log.size()
			continue
		}
		prevIndex := rf.nextIndex[peer] - 1
		if prevIndex < rf.log.snapLastIndex {
			args := &InstallSnapshotArgs{
				Term:              term,
				LeaderID:          rf.me,
				LastIncludedTerm:  rf.log.snapLastTerm,
				LastIncludedIndex: rf.log.snapLastIndex,
				Snapshot:          rf.log.snapshot,
			}
			LOG(rf.me, rf.currentTerm, DLog, "-> S%d, InstallSnapshot", peer)
			go rf.installToPeer(peer, term, args)
		} else {
			args := &AppendEntriesArgs{
				Term:         term,
				LeaderID:     rf.me,
				PrevLogIndex: prevIndex,
				PrevLogTerm:  rf.log.at(prevIndex).Term,
				Entries:      rf.log.tail(prevIndex + 1),
				LeaderCommit: rf.commitIndex,
			}

			go replicateToPeer(peer, args)
		}
	}

	return true
}
