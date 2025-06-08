package kvraft

import (
	"bytes"
	"course/labgob"
	"course/labrpc"
	"course/raft"
	"sync"
	"sync/atomic"
	"time"
)

type KVServer struct {
	mu               sync.Mutex
	me               int
	rf               *raft.Raft
	applyCh          chan raft.ApplyMsg
	dead             int32
	maxRaftState     int
	lastAppliedIndex int
	stateMachine     *MemoryKVStateMachine
	notifyChan       map[int]chan *OpReply
	deduplicateTable map[int64]*LastOperationInfo
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// pass command to raft
	index, _, isLeader := kv.rf.Start(Op{
		Key:           args.Key,
		OperationType: OpGet,
	})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	//wait for raft to apply command
	kv.mu.Lock()
	notifyCh := kv.getNotifyChannelLocked(index)
	kv.mu.Unlock()

	select {
	case result := <-notifyCh:
		reply.Value = result.Value
		reply.Err = result.Err
	case <-time.After(ClientRequestTimeout):
		reply.Err = ErrTimeout
	}

	go kv.removeNotifyChannel(index)

}

func (kv *KVServer) isRequestDuplicateLocked(clientID, seqID int64) bool {
	info, ok := kv.deduplicateTable[clientID]
	return ok && info.SeqID >= seqID
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	//check if request is duplicate
	kv.mu.Lock()
	if kv.isRequestDuplicateLocked(args.ClientId, args.SeqId) {
		reply.Err = kv.deduplicateTable[args.ClientId].Reply.Err
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	// pass command to raft
	index, _, isLeader := kv.rf.Start(Op{
		Key:           args.Key,
		Value:         args.Value,
		OperationType: getOperationType(args.Op),
		ClientID:      args.ClientId,
		SeqID:         args.SeqId,
	})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	//wait for raft to apply command
	kv.mu.Lock()
	notifyCh := kv.getNotifyChannelLocked(index)
	kv.mu.Unlock()

	//now := time.Now()
	select {
	case result := <-notifyCh:
		reply.Err = result.Err
	case <-time.After(ClientRequestTimeout):
		reply.Err = ErrTimeout
		//log.Printf("PutAppend timeout: %v\n", time.Since(now))
	}

	go kv.removeNotifyChannel(index)
}

// Kill the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxRaftState bytes,
// in order to allow Raft to garbage-collect its log. if maxRaftState is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxRaftState int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxRaftState = maxRaftState

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.dead = 0
	kv.stateMachine = NewMemoryKVStateMachine()
	kv.notifyChan = make(map[int]chan *OpReply)
	kv.deduplicateTable = make(map[int64]*LastOperationInfo)

	// restore data from snapshot
	kv.restoreFromSnapshot(persister.ReadSnapshot())

	go kv.applyTask()
	return kv
}

func (kv *KVServer) applyTask() {
	for !kv.killed() {
		select {
		case msg := <-kv.applyCh:
			if msg.CommandValid {
				kv.mu.Lock()
				// omit handled command
				if msg.CommandIndex <= kv.lastAppliedIndex {
					kv.mu.Unlock()
					continue
				}
				kv.lastAppliedIndex = msg.CommandIndex

				// apply operation to kv state machine
				op := msg.Command.(Op)
				var opReply *OpReply
				if op.OperationType != OpGet && kv.isRequestDuplicateLocked(op.ClientID, op.SeqID) {
					opReply = kv.deduplicateTable[op.ClientID].Reply
				} else {
					opReply = kv.applyToStateMachine(op)
					if op.OperationType != OpGet {
						kv.deduplicateTable[op.ClientID] = &LastOperationInfo{
							SeqID: op.SeqID,
							Reply: opReply,
						}
					}
				}
				// send reply
				if _, isLeader := kv.rf.GetState(); isLeader {
					kv.getNotifyChannelLocked(msg.CommandIndex) <- opReply
				}

				// check if snapshot is needed
				if kv.maxRaftState != -1 && kv.rf.GetRaftStateSize() >= kv.maxRaftState {
					kv.makeSnapshot(msg.CommandIndex)
				}
				kv.mu.Unlock()
			} else if msg.SnapshotValid {
				kv.mu.Lock()
				kv.restoreFromSnapshot(msg.Snapshot)
				kv.lastAppliedIndex = msg.SnapshotIndex
				kv.mu.Unlock()
			}

		}
	}
}

func (kv *KVServer) applyToStateMachine(op Op) *OpReply {
	var (
		value string
		err   Err
	)
	switch op.OperationType {
	case OpGet:
		value, err = kv.stateMachine.Get(op.Key)
	case OpPut:
		err = kv.stateMachine.Put(op.Key, op.Value)
	case OpAppend:
		err = kv.stateMachine.Append(op.Key, op.Value)
	}
	return &OpReply{
		Value: value,
		Err:   err,
	}
}

func (kv *KVServer) getNotifyChannelLocked(index int) chan *OpReply {
	if kv.notifyChan[index] == nil {
		kv.notifyChan[index] = make(chan *OpReply, 1)
	}
	return kv.notifyChan[index]
}

func (kv *KVServer) removeNotifyChannel(index int) {
	kv.mu.Lock()
	delete(kv.notifyChan, index)
	kv.mu.Unlock()
}

func (kv *KVServer) makeSnapshot(index int) {
	buf := new(bytes.Buffer)
	enc := labgob.NewEncoder(buf)
	_ = enc.Encode(kv.stateMachine)
	_ = enc.Encode(kv.deduplicateTable)
	kv.rf.Snapshot(index, buf.Bytes())
}

func (kv *KVServer) restoreFromSnapshot(snapshot []byte) {
	if len(snapshot) == 0 {
		return
	}

	buf := bytes.NewBuffer(snapshot)
	dec := labgob.NewDecoder(buf)
	var (
		stateMachine     *MemoryKVStateMachine
		deduplicateTable map[int64]*LastOperationInfo
	)
	if dec.Decode(&stateMachine) != nil || dec.Decode(&deduplicateTable) != nil {
		panic("fail to restore data from snapshot")
	}

	kv.stateMachine = stateMachine
	kv.deduplicateTable = deduplicateTable
}
