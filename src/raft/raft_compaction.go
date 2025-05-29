package raft

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (PartD).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.log.snapLastIndex || index > rf.commitIndex {
		LOG(rf.me, rf.currentTerm, DSnap, "Could not snapshot beyond [%d, %d]", rf.log.snapLastIndex+1, rf.commitIndex)
		return
	}

	LOG(rf.me, rf.currentTerm, DSnap, "Snapshot to %d", index)
	rf.log.doSnapshot(index, snapshot)
	rf.persistLocked()
}

type InstallSnapshotArgs struct {
	Term     int
	LeaderID int

	// log entries
	LastIncludedIndex int
	LastIncludedTerm  int

	Snapshot []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	// align term
	if args.Term < reply.Term {
		LOG(rf.me, rf.currentTerm, DSnap, "-> S%d, Reject, higher term, T%d->T%d", args.LeaderID, rf.currentTerm, args.Term)
		return
	}

	if args.Term > rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	}

	if rf.log.snapLastIndex >= args.LastIncludedIndex {
		LOG(rf.me, rf.currentTerm, DSnap, "-> S%d, Reject, snapshot already installed", args.LeaderID)
		return
	}

	rf.log.installSnapshot(args.LastIncludedIndex, args.LastIncludedTerm, args.Snapshot)
	rf.persistLocked()
	rf.snapPending = true
	rf.applyCond.Signal()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) installToPeer(peer, term int, args *InstallSnapshotArgs) {
	reply := new(InstallSnapshotReply)
	ok := rf.sendInstallSnapshot(peer, args, reply)

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
		LOG(rf.me, rf.currentTerm, DLog, "Lost context, abort InstallSnapshot for S%d", rf.me)
		return
	}

	// update match/next index if success
	if args.LastIncludedIndex > rf.matchIndex[peer] {
		rf.matchIndex[peer] = args.LastIncludedIndex
		rf.nextIndex[peer] = args.LastIncludedIndex + 1
	}

	// note: we need not update commitIndex here
	// because commitIndex > snapshotLastIndex
}
