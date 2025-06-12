package kvraft

import (
	"course/labrpc"
	"crypto/rand"
	"math/big"
)

type Clerk struct {
	servers  []*labrpc.ClientEnd
	leaderId int

	// clientId + seqId as unique key for command
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

func (clerk *Clerk) Get(key string) string {
	args := GetArgs{Key: key}

	for {
		var reply GetReply
		ok := clerk.servers[clerk.leaderId].Call("KVServer.Get", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			clerk.leaderId = (clerk.leaderId + 1) % len(clerk.servers)
			continue
		}
		return reply.Value
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
		var reply PutAppendReply
		ok := clerk.servers[clerk.leaderId].Call("KVServer.PutAppend", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			clerk.leaderId = (clerk.leaderId + 1) % len(clerk.servers)
			continue
		}
		clerk.seqId++
		return
	}
}

func (clerk *Clerk) Put(key string, value string) {
	clerk.PutAppend(key, value, "Put")
}

func (clerk *Clerk) Append(key string, value string) {
	clerk.PutAppend(key, value, "Append")
}
