package shardkv

import (
	"course/shardctrler"
	"time"
)

func (kv *ShardKV) ConfigCommand(command RaftCommand, reply *OpReply) {
	index, _, isLeader := kv.rf.Start(command)
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
	}

	go kv.removeNotifyChannel(index)
}

func (kv *ShardKV) handleConfigChangeMessage(raftCommand RaftCommand) *OpReply {
	switch raftCommand.CommandType {
	case ConfigurationChange:
		config := raftCommand.Data.(shardctrler.Config)
		return kv.applyNewConfig(config)
	case ShardMigration:
		shardData := raftCommand.Data.(ShardOperationReply)
		return kv.applyShardMigration(shardData)
	case ShardGC:
		shardData := raftCommand.Data.(ShardOperationArgs)
		return kv.applyShardGC(&shardData)
	default:
		panic("unexpected command type")

	}
}

func (kv *ShardKV) applyNewConfig(newConfig shardctrler.Config) *OpReply {
	if kv.currentConfig.Num+1 != newConfig.Num {
		return &OpReply{
			Err: ErrWrongConfig,
		}
	}
	for i := 0; i < shardctrler.NShards; i++ {
		// newConfig move out
		if kv.currentConfig.Shards[i] == kv.gid && newConfig.Shards[i] != kv.gid {
			gid := newConfig.Shards[i]
			if gid != 0 {
				kv.shards[i].Status = MoveOut
			}
		}
		// newConfig move in
		if kv.currentConfig.Shards[i] != kv.gid && newConfig.Shards[i] == kv.gid {
			gid := kv.currentConfig.Shards[i]
			if gid != 0 {
				kv.shards[i].Status = MoveIn
			}
		}
	}
	kv.prevConfig = kv.currentConfig
	kv.currentConfig = newConfig
	return OKReply
}
