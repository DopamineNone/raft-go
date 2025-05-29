package raft

func (rf *Raft) applicationTicker() {
	for !rf.killed() {
		rf.mu.Lock()
		rf.applyCond.Wait()

		snapPendingApply := rf.snapPending
		entries := make([]LogEntry, 0)
		if !snapPendingApply {
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				entries = append(entries, rf.log.at(i))
			}
		}

		rf.mu.Unlock()

		if !snapPendingApply {
			for i, entry := range entries {
				rf.applyCh <- ApplyMsg{
					CommandValid: entry.CommandValid,
					Command:      entry.Command,
					CommandIndex: rf.lastApplied + i + 1,
				}
			}
		} else {
			rf.applyCh <- ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.log.snapshot,
				SnapshotTerm:  rf.log.snapLastTerm,
				SnapshotIndex: rf.log.snapLastIndex,
			}
		}

		rf.mu.Lock()
		if !snapPendingApply {
			LOG(rf.me, rf.currentTerm, DLog, "-> Apply %d entries", len(entries))
			rf.lastApplied += len(entries)
		} else {
			rf.lastApplied = rf.log.snapLastIndex
			if rf.commitIndex < rf.lastApplied {
				rf.commitIndex = rf.lastApplied
			}
			rf.snapPending = false
			LOG(rf.me, rf.currentTerm, DLog, "-> Apply snapshot")
		}
		rf.mu.Unlock()
	}
}
