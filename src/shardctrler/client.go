package shardctrler

import (
	"course/labrpc"
	"crypto/rand"
	"math/big"
)

type Clerk struct {
	servers []*labrpc.ClientEnd

	leaderId int
	clientId int64
	seqId    int64
}

// nrand: get random number
func nrand() int64 {
	maxNum := big.NewInt(int64(1) << 62)
	bigNum, _ := rand.Int(rand.Reader, maxNum)
	return bigNum.Int64()
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	return &Clerk{
		servers:  servers,
		leaderId: 0,
		clientId: nrand(),
		seqId:    0,
	}
}

func (clerk *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{Servers: servers, ClientId: clerk.clientId, SeqId: clerk.seqId}

	for {
		var reply JoinReply
		ok := clerk.servers[clerk.leaderId].Call("ShardController.Join", args, &reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			clerk.leaderId = (clerk.leaderId + 1) % len(clerk.servers)
			continue
		}
		clerk.seqId++
		return
	}
}

func (clerk *Clerk) Leave(gids []int) {
	args := &LeaveArgs{GIDs: gids, ClientId: clerk.clientId, SeqId: clerk.seqId}

	for {
		var reply LeaveReply
		ok := clerk.servers[clerk.leaderId].Call("ShardController.Leave", args, &reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			clerk.leaderId = (clerk.leaderId + 1) % len(clerk.servers)
			continue
		}
		clerk.seqId++
		return
	}
}

func (clerk *Clerk) Query(num int) Config {
	args := &QueryArgs{Num: num}

	for {
		var reply QueryReply
		ok := clerk.servers[clerk.leaderId].Call("ShardController.Query", args, &reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			clerk.leaderId = (clerk.leaderId + 1) % len(clerk.servers)
			continue
		}
		return reply.Config
	}
}

func (clerk *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{Shard: shard, GID: gid, ClientId: clerk.clientId, SeqId: clerk.seqId}

	for {
		var reply MoveReply
		ok := clerk.servers[clerk.leaderId].Call("ShardController.Move", args, &reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			clerk.leaderId = (clerk.leaderId + 1) % len(clerk.servers)
			continue
		}
		clerk.seqId++
		return
	}
}
