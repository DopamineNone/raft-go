package shardctrler

import (
	"course/labgob"
	"course/labrpc"
	"course/raft"
	"sync"
	"sync/atomic"
	"time"
)

// Config is immutable configuration --
// an assignment of shards to groups

type ShardController struct {
	mu               sync.Mutex
	me               int
	rf               *raft.Raft
	applyCh          chan raft.ApplyMsg
	config           []Config
	lastAppliedIndex int
	stateMachine     *ControllerStateMachine
	notifyChan       map[int]chan *OpReply
	deduplicateTable map[int64]*LastOperationInfo
	dead             int32
}

func (sc *ShardController) Join(args *JoinArgs, reply *JoinReply) {
	opReply := new(OpReply)
	sc.execCommand(Op{
		OperationType: OpJoin,
		ClientId:      args.ClientId,
		SeqId:         args.SeqId,
		Servers:       args.Servers,
	}, opReply)

	reply.Err = opReply.Err
}

func (sc *ShardController) Leave(args *LeaveArgs, reply *LeaveReply) {
	opReply := new(OpReply)
	sc.execCommand(Op{
		OperationType: OpLeave,
		ClientId:      args.ClientId,
		SeqId:         args.SeqId,
		GIDs:          args.GIDs,
	}, opReply)

	reply.Err = opReply.Err
}

func (sc *ShardController) Query(args *QueryArgs, reply *QueryReply) {
	opReply := new(OpReply)
	sc.execCommand(Op{
		OperationType: OpQuery,
		Num:           args.Num,
	}, opReply)

	reply.Config = opReply.ControllerConfig
	reply.Err = opReply.Err
}

func (sc *ShardController) Move(args *MoveArgs, reply *MoveReply) {
	opReply := new(OpReply)
	sc.execCommand(Op{
		OperationType: OpMove,
		ClientId:      args.ClientId,
		SeqId:         args.SeqId,
		Shard:         args.Shard,
		GID:           args.GID,
	}, opReply)

	reply.Err = opReply.Err
}

func (sc *ShardController) execCommand(args Op, reply *OpReply) {
	//check if request is duplicate
	sc.mu.Lock()
	if args.OperationType != OpQuery && sc.isRequestDuplicateLocked(args.ClientId, args.SeqId) {
		reply.Err = sc.deduplicateTable[args.ClientId].Reply.Err
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()
	// pass command to raft
	index, _, isLeader := sc.rf.Start(args)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	//wait for raft to apply command
	sc.mu.Lock()
	notifyCh := sc.getNotifyChannelLocked(index)
	sc.mu.Unlock()

	//now := time.Now()
	select {
	case result := <-notifyCh:
		reply.ControllerConfig = result.ControllerConfig
		reply.Err = result.Err
	case <-time.After(ClientRequestTimeout):
		reply.Err = ErrTimeout
		//log.Printf("PutAppend timeout: %v\n", time.Since(now))
	}

	go sc.removeNotifyChannel(index)

}

// Kill the tester calls Kill() when a ShardController instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (sc *ShardController) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardController) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

func (sc *ShardController) Raft() *raft.Raft {
	return sc.rf
}

// StartShardController servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxRaftState bytes,
// in order to allow Raft to garbage-collect its log. if maxRaftState is -1,
// you don't need to snapshot.
// StartShardController() must return quickly, so it should start goroutines
// for any long-running work.
func StartShardController(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardController {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	sc := new(ShardController)
	sc.me = me

	// You may need initialization code here.

	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// You may need initialization code here.
	sc.dead = 0
	sc.stateMachine = NewControllerStateMachine()
	sc.notifyChan = make(map[int]chan *OpReply)
	sc.deduplicateTable = make(map[int64]*LastOperationInfo)

	go sc.applyTask()
	return sc
}

func (sc *ShardController) applyTask() {
	for !sc.killed() {
		select {
		case msg := <-sc.applyCh:
			if msg.CommandValid {
				sc.mu.Lock()
				// omit handled command
				if msg.CommandIndex <= sc.lastAppliedIndex {
					sc.mu.Unlock()
					continue
				}
				sc.lastAppliedIndex = msg.CommandIndex

				// apply operation to sc state machine
				op := msg.Command.(Op)
				var opReply *OpReply
				if op.OperationType != OpQuery && sc.isRequestDuplicateLocked(op.ClientId, op.SeqId) {
					opReply = sc.deduplicateTable[op.ClientId].Reply
				} else {
					opReply = sc.applyToStateMachine(op)
					if op.OperationType != OpQuery {
						sc.deduplicateTable[op.ClientId] = &LastOperationInfo{
							SeqId: op.SeqId,
							Reply: opReply,
						}
					}
				}
				// send reply
				if _, isLeader := sc.rf.GetState(); isLeader {
					sc.getNotifyChannelLocked(msg.CommandIndex) <- opReply
				}

				sc.mu.Unlock()
			}
		}
	}
}

func (sc *ShardController) applyToStateMachine(op Op) *OpReply {
	var (
		config Config
		err    Err
	)
	switch op.OperationType {
	case OpJoin:
		err = sc.stateMachine.Join(op.Servers)
	case OpLeave:
		err = sc.stateMachine.Leave(op.GIDs)
	case OpMove:
		err = sc.stateMachine.Move(op.Shard, op.GID)
	case OpQuery:
		config, err = sc.stateMachine.Query(op.Num)
	}

	return &OpReply{
		ControllerConfig: config,
		Err:              err,
	}
}

func (sc *ShardController) getNotifyChannelLocked(index int) chan *OpReply {
	if sc.notifyChan[index] == nil {
		sc.notifyChan[index] = make(chan *OpReply, 1)
	}
	return sc.notifyChan[index]
}

func (sc *ShardController) removeNotifyChannel(index int) {
	sc.mu.Lock()
	delete(sc.notifyChan, index)
	sc.mu.Unlock()
}

func (sc *ShardController) isRequestDuplicateLocked(clientID, seqID int64) bool {
	info, ok := sc.deduplicateTable[clientID]
	return ok && info.SeqId >= seqID
}
