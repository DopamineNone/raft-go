package raft

import (
	"math/rand"
	"time"
)

func (rf *Raft) resetElectionTimeLocked() {
	rf.electionStart = time.Now()
	rf.electionTimeout = electionTimeoutMin + time.Duration(rand.Int63()%electionRandRange)
}

func (rf *Raft) isElectionTimeoutLocked() bool {
	return time.Since(rf.electionStart) > rf.electionTimeout
}

func (rf *Raft) isMoreUpToDateLocked(candidateTerm int, candidateIndex int) bool {
	lastIndex, lastTerm := rf.log.last()

	LOG(rf.me, rf.currentTerm, DVote, "-> S%d, LastLogIndex: %d, LastLogTerm: %d, CandidateLogIndex: %d, CandidateLogTerm: %d", rf.me, lastIndex, lastTerm, candidateTerm, candidateIndex)

	if lastTerm != candidateTerm {
		return lastTerm > candidateTerm
	}
	return lastIndex > candidateIndex
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (PartA, PartB).
	Term        int
	CandidateID int

	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (PartA).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (PartA, PartB).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if args.Term < reply.Term {
		LOG(rf.me, rf.currentTerm, DVote, "-> S%d, Reject voted, higher term, T%d->T%d", args.CandidateID, rf.currentTerm, args.Term)
		return
	}
	if args.Term > reply.Term {
		rf.becomeFollowerLocked(args.Term)
	}

	if rf.voteFor != -1 {
		LOG(rf.me, rf.currentTerm, DVote, "-> S%d, Reject, Already voted to S%d", args.CandidateID, rf.voteFor)
		return
	}

	// check if candidate log is more up to date
	if rf.isMoreUpToDateLocked(args.LastLogTerm, args.LastLogIndex) {
		LOG(rf.me, rf.currentTerm, DVote, "-> S%d, Reject, Up to date", args.CandidateID)
		return
	}

	reply.VoteGranted = true
	rf.voteFor = args.CandidateID
	rf.persistLocked()
	rf.resetElectionTimeLocked()
	LOG(rf.me, rf.currentTerm, DVote, "-> S%d, Vote granted", args.CandidateID)
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) startElection(term int) {
	votes := 1
	askVoteFromPeer := func(peer int, args *RequestVoteArgs) {
		reply := new(RequestVoteReply)
		ok := rf.sendRequestVote(peer, args, reply)

		rf.mu.Lock()
		defer rf.mu.Unlock()

		if !ok {
			LOG(rf.me, rf.currentTerm, DDebug, "Ask vote from S%d, Lost or error", peer)
			return
		}

		if reply.Term > rf.currentTerm {
			rf.becomeFollowerLocked(reply.Term)
			return
		}

		if rf.contextLostLocked(Candidate, term) {
			LOG(rf.me, rf.currentTerm, DVote, "Lost context, abort RequestVoteReply for S%d", peer)
			return
		}

		if reply.VoteGranted {
			votes++
			if votes > len(rf.peers)/2 {
				rf.becomeLeaderLocked()
				go rf.replicationTicker(term)
			}
		}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.contextLostLocked(Candidate, term) {
		LOG(rf.me, rf.currentTerm, DVote, "Lost Candidate to %s, abort RequestVoteReply", rf.role)
		return
	}
	lastIndex, lastTerm := rf.log.last()
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		args := &RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateID:  rf.me,
			LastLogIndex: lastIndex,
			LastLogTerm:  lastTerm,
		}
		go askVoteFromPeer(peer, args)
	}
}

func (rf *Raft) electionTicker() {
	for !rf.killed() {

		// Your code here (PartA)
		// Check if a leader election should be started.
		rf.mu.Lock()
		if rf.role != Leader && rf.isElectionTimeoutLocked() {
			rf.becomeCandidateLocked()
			go rf.startElection(rf.currentTerm)
		}
		rf.mu.Unlock()
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}
