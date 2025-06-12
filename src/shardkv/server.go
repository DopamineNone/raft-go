package shardkv

import (
	"course/labgob"
	"course/labrpc"
	"course/raft"
	"course/shardctrler"
	"sync"
	"sync/atomic"
	"time"
)

type ShardKV struct {
	mu               sync.Mutex
	me               int
	rf               *raft.Raft
	applyCh          chan raft.ApplyMsg
	make_end         func(string) *labrpc.ClientEnd
	gid              int
	ctrlers          []*labrpc.ClientEnd
	dead             int32
	maxRaftState     int
	lastAppliedIndex int
	shards           map[int]*MemoryKVStateMachine
	notifyChan       map[int]chan *OpReply
	deduplicateTable map[int64]*LastOperationInfo
	currentConfig    shardctrler.Config
	prevConfig       shardctrler.Config
	mck              *shardctrler.Clerk
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	if !kv.matchGroup(args.Key) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	// pass command to raft
	index, _, isLeader := kv.rf.Start(RaftCommand{
		CommandType: ClientOperation,
		Data: Op{
			Key:           args.Key,
			OperationType: OpGet,
		},
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

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	if !kv.matchGroup(args.Key) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	//check if request is duplicate
	if kv.isRequestDuplicateLocked(args.ClientId, args.SeqId) {
		reply.Err = kv.deduplicateTable[args.ClientId].Reply.Err
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	// pass command to raft
	index, _, isLeader := kv.rf.Start(RaftCommand{
		CommandType: ClientOperation,
		Data: Op{
			Key:           args.Key,
			Value:         args.Value,
			OperationType: getOperationType(args.Op),
			ClientId:      args.ClientId,
			SeqId:         args.SeqId,
		},
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

func (kv *ShardKV) isRequestDuplicateLocked(clientID, seqID int64) bool {
	info, ok := kv.deduplicateTable[clientID]
	return ok && info.SeqID >= seqID
}

func (kv *ShardKV) matchGroup(key string) bool {
	shardId := key2shard(key)
	status := kv.shards[shardId].Status
	return kv.currentConfig.Shards[shardId] == kv.gid && (status == GC || status == Normal)
}

// Kill the tester calls Kill() when a ShardKV instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
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
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxRaftState int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(RaftCommand{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(ShardOperationArgs{})
	labgob.Register(ShardOperationReply{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxRaftState = maxRaftState
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.dead = 0
	kv.shards = make(map[int]*MemoryKVStateMachine)
	kv.notifyChan = make(map[int]chan *OpReply)
	kv.deduplicateTable = make(map[int64]*LastOperationInfo)
	kv.currentConfig = shardctrler.DefaultConfig()
	kv.prevConfig = shardctrler.DefaultConfig()

	// restore data from snapshot
	kv.restoreFromSnapshot(persister.ReadSnapshot())

	go kv.applyTask()
	go kv.fetchConfigTask()
	go kv.shardMigrateTask()
	go kv.shardGCTask()
	return kv
}
