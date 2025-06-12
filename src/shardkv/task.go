package shardkv

import (
	"bytes"
	"course/labgob"
	"course/shardctrler"
	"maps"
	"sync"
	"time"
)

func (kv *ShardKV) applyTask() {
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

				raftCommand := msg.Command.(RaftCommand)
				var opReply *OpReply
				if raftCommand.CommandType == ClientOperation {
					// apply operation to kv state machine
					op := raftCommand.Data.(Op)
					opReply = kv.applyClientRequest(op)
				} else {
					opReply = kv.handleConfigChangeMessage(raftCommand)
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

func (kv *ShardKV) applyClientRequest(op Op) (opReply *OpReply) {
	if !kv.matchGroup(op.Key) {
		return &OpReply{Err: ErrWrongGroup}
	}
	if op.OperationType != OpGet && kv.isRequestDuplicateLocked(op.ClientId, op.SeqId) {
		opReply = kv.deduplicateTable[op.ClientId].Reply
	} else {
		shard := key2shard(op.Key)
		opReply = kv.applyToStateMachine(op, shard)
		if op.OperationType != OpGet {
			kv.deduplicateTable[op.ClientId] = &LastOperationInfo{
				SeqID: op.SeqId,
				Reply: opReply,
			}
		}
	}
	return
}

func (kv *ShardKV) applyToStateMachine(op Op, shardId int) *OpReply {
	var (
		value string
		err   Err
	)
	switch op.OperationType {
	case OpGet:
		value, err = kv.shards[shardId].Get(op.Key)
	case OpPut:
		err = kv.shards[shardId].Put(op.Key, op.Value)
	case OpAppend:
		err = kv.shards[shardId].Append(op.Key, op.Value)
	}
	return &OpReply{
		Value: value,
		Err:   err,
	}
}

func (kv *ShardKV) getNotifyChannelLocked(index int) chan *OpReply {
	if kv.notifyChan[index] == nil {
		kv.notifyChan[index] = make(chan *OpReply, 1)
	}
	return kv.notifyChan[index]
}

func (kv *ShardKV) removeNotifyChannel(index int) {
	kv.mu.Lock()
	delete(kv.notifyChan, index)
	kv.mu.Unlock()
}

func (kv *ShardKV) makeSnapshot(index int) {
	buf := new(bytes.Buffer)
	enc := labgob.NewEncoder(buf)
	_ = enc.Encode(kv.shards)
	_ = enc.Encode(kv.deduplicateTable)
	_ = enc.Encode(kv.currentConfig)
	_ = enc.Encode(kv.prevConfig)
	kv.rf.Snapshot(index, buf.Bytes())
}

func (kv *ShardKV) restoreFromSnapshot(snapshot []byte) {
	if len(snapshot) == 0 {
		for i := 0; i < shardctrler.NShards; i++ {
			if _, ok := kv.shards[i]; !ok {
				kv.shards[i] = NewMemoryKVStateMachine()
			}
		}
		return
	}

	buf := bytes.NewBuffer(snapshot)
	dec := labgob.NewDecoder(buf)
	var (
		stateMachine     map[int]*MemoryKVStateMachine
		deduplicateTable map[int64]*LastOperationInfo
		currentConfig    shardctrler.Config
		prevConfig       shardctrler.Config
	)
	if dec.Decode(&stateMachine) != nil ||
		dec.Decode(&deduplicateTable) != nil ||
		dec.Decode(&currentConfig) != nil ||
		dec.Decode(&prevConfig) != nil {
		panic("fail to restore data from snapshot")
	}

	kv.shards = stateMachine
	kv.deduplicateTable = deduplicateTable
	kv.currentConfig = currentConfig
	kv.prevConfig = prevConfig
}

func (kv *ShardKV) fetchConfigTask() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			needFetch := true
			kv.mu.Lock()
			for _, shard := range kv.shards {
				if shard.Status != Normal {
					needFetch = false
					break
				}
			}
			configNum := kv.currentConfig.Num
			kv.mu.Unlock()
			if needFetch {
				newConfig := kv.mck.Query(configNum + 1)
				if newConfig.Num == configNum+1 {
					// update config using raft
					kv.ConfigCommand(RaftCommand{
						CommandType: ConfigurationChange,
						Data:        newConfig,
					}, &OpReply{})
				}
			}

		}
		time.Sleep(FetchConfigInterval)
	}
}

func (kv *ShardKV) shardMigrateTask() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.Lock()
			gitToShards := kv.getShardByStatus(MoveIn)
			wg := &sync.WaitGroup{}
			for gid, shards := range gitToShards {
				wg.Add(1)
				go func(servers []string, configNum int, shards []int) {
					defer wg.Done()
					// fetch shards from groups that need to be migrated
					args := &ShardOperationArgs{
						ConfigNum: configNum,
						ShardIds:  shards,
					}
					for _, server := range servers {
						var reply ShardOperationReply
						ok := kv.make_end(server).Call("ShardKV.GetShardData", args, &reply)
						if ok && reply.Err == OK {
							kv.ConfigCommand(RaftCommand{
								CommandType: ShardMigration,
								Data:        reply,
							}, &OpReply{})
						}
					}
				}(kv.prevConfig.Groups[gid], kv.currentConfig.Num, shards)
			}
			kv.mu.Unlock()
			wg.Wait()
		}
		time.Sleep(ShardMigrateInterval)
	}
}

func (kv *ShardKV) applyShardMigration(data ShardOperationReply) *OpReply {
	if data.ConfigNum != kv.currentConfig.Num {
		return &OpReply{Err: ErrWrongConfig}
	}
	// move in shard data
	for shardId, data := range data.ShardData {
		shard := kv.shards[shardId]
		if shard.Status != MoveIn {
			break
		}
		for key, value := range data {
			shard.Put(key, value)
		}
		shard.Status = GC
	}
	// update dup table
	for clientId, info := range data.DupTable {
		mInfo, ok := kv.deduplicateTable[clientId]
		if !ok || mInfo.SeqID < info.SeqID {
			kv.deduplicateTable[clientId] = info
		}
	}
	return OKReply
}

func (kv *ShardKV) getShardByStatus(status ShardStatus) map[int][]int {
	gidToShards := make(map[int][]int)
	for id, shard := range kv.shards {
		if shard.Status == status {
			gid := kv.prevConfig.Shards[id]
			if gid != 0 {
				gidToShards[gid] = append(gidToShards[gid], id)
			}
		}
	}
	return gidToShards
}

func (kv *ShardKV) GetShardData(args *ShardOperationArgs, reply *ShardOperationReply) {
	// check if leader
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	// get config version
	if args.ConfigNum > kv.currentConfig.Num {
		reply.Err = ErrNotReady
		return
	}
	reply.ConfigNum = kv.currentConfig.Num

	// get shard data
	if reply.ShardData == nil {
		reply.ShardData = make(map[int]map[string]string)
	}
	for _, shardId := range args.ShardIds {
		if reply.ShardData[shardId] == nil {
			reply.ShardData[shardId] = make(map[string]string)
		}
		maps.Copy(reply.ShardData[shardId], kv.shards[shardId].Mp)
	}

	// get dup table
	if reply.DupTable == nil {
		reply.DupTable = make(map[int64]*LastOperationInfo)
	}
	for clientId, info := range kv.deduplicateTable {
		reply.DupTable[clientId] = info.copy()
	}

	reply.Err = OK
}

func (kv *ShardKV) shardGCTask() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.Lock()
			gidToShard := kv.getShardByStatus(GC)
			wg := new(sync.WaitGroup)
			for gid, shards := range gidToShard {
				wg.Add(1)
				go func(servers []string, configNum int, shards []int) {
					defer wg.Done()
					args := ShardOperationArgs{
						ConfigNum: configNum,
						ShardIds:  shards,
					}
					for _, server := range servers {
						var reply ShardOperationReply
						ok := kv.make_end(server).Call("ShardKV.DeleteShardData", &args, &reply)
						if ok && reply.Err == OK {
							kv.ConfigCommand(RaftCommand{
								CommandType: ShardGC,
								Data:        args,
							}, &OpReply{})
						}
					}
				}(kv.prevConfig.Groups[gid], kv.currentConfig.Num, shards)
			}
			kv.mu.Unlock()
			wg.Wait()
		}
		time.Sleep(ShardGCInterval)
	}
}

func (kv *ShardKV) DeleteShardData(args *ShardOperationArgs, reply *ShardOperationReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	reply.Err = OK

	//kv.mu.Lock()
	//if args.ConfigNum < kv.currentConfig.Num {
	//	kv.mu.Unlock()
	//	return
	//}
	//kv.mu.Unlock()

	kv.ConfigCommand(RaftCommand{
		CommandType: ShardGC,
		Data:        *args,
	}, &OpReply{})
}

func (kv *ShardKV) applyShardGC(info *ShardOperationArgs) *OpReply {
	if info.ConfigNum != kv.currentConfig.Num {
		return &OpReply{Err: ErrWrongConfig}
	}
	for _, shardId := range info.ShardIds {
		shard := kv.shards[shardId]
		switch shard.Status {
		case GC:
			shard.Status = Normal
		case MoveOut:
			kv.shards[shardId] = NewMemoryKVStateMachine()
		default:
			break
		}
	}
	return OKReply
}
