package shardkv

import (
	"course/labrpc"
	"course/shardctrler"
	"crypto/rand"
	"math/big"
	"time"
)

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd

	leaderIds map[int]int

	// clientId + seqId as unique key for command
	clientId int64
	seqId    int64
}

func key2shard(key string) int {
	shard := 0
	if len(key) > shard {
		shard = int(key[0]) % shardctrler.NShards
	}
	return shard
}

// nrand: get random number
func nrand() int64 {
	maxNum := big.NewInt(int64(1) << 62)
	bigNum, _ := rand.Int(rand.Reader, maxNum)
	return bigNum.Int64()
}

func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	return &Clerk{
		sm:        shardctrler.MakeClerk(ctrlers),
		make_end:  make_end,
		leaderIds: make(map[int]int),
		clientId:  nrand(),
		seqId:     0,
	}
}

func (clerk *Clerk) Get(key string) string {
	args := GetArgs{Key: key}

	for {
		shard := key2shard(key)
		gid := clerk.config.Shards[shard]
		if servers, ok := clerk.config.Groups[gid]; ok {
			if _, exist := clerk.leaderIds[shard]; !exist {
				clerk.leaderIds[gid] = 0
			}
			oldLeaderId := clerk.leaderIds[gid]
			for {
				srv := clerk.make_end(servers[clerk.leaderIds[gid]])
				var reply GetReply
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					return reply.Value
				}
				if ok && reply.Err == ErrWrongGroup {
					break
				}
				if !ok || reply.Err == ErrTimeout || reply.Err == ErrWrongLeader {
					clerk.leaderIds[gid] = (clerk.leaderIds[gid] + 1) % len(servers)
					if oldLeaderId == clerk.leaderIds[gid] {
						break
					}
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		clerk.config = clerk.sm.Query(-1)
	}
}

func (clerk *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: clerk.clientId,
		SeqId:    clerk.seqId,
	}
	for {
		shard := key2shard(key)
		gid := clerk.config.Shards[shard]
		if servers, ok := clerk.config.Groups[gid]; ok {
			if _, exist := clerk.leaderIds[shard]; !exist {
				clerk.leaderIds[gid] = 0
			}
			oldLeaderId := clerk.leaderIds[gid]
			for {
				srv := clerk.make_end(servers[clerk.leaderIds[gid]])
				var reply GetReply
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					clerk.seqId++
					return
				}
				if ok && reply.Err == ErrWrongGroup {
					break
				}
				if !ok || reply.Err == ErrTimeout || reply.Err == ErrWrongLeader {
					clerk.leaderIds[gid] = (clerk.leaderIds[gid] + 1) % len(servers)
					if oldLeaderId == clerk.leaderIds[gid] {
						break
					}
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		clerk.config = clerk.sm.Query(-1)
	}
}

func (clerk *Clerk) Put(key string, value string) {
	clerk.PutAppend(key, value, "Put")
}

func (clerk *Clerk) Append(key string, value string) {
	clerk.PutAppend(key, value, "Append")
}
