package kvraft

import (
	"course/labrpc"
	"crypto/rand"
	"math/big"
)

type Clerk struct {
	servers  []*labrpc.ClientEnd
	leaderID int

	// clientID + seqID as unique key for command
	clientID int64
	seqID    int64
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
		leaderID: 0,
		clientID: nrand(),
		seqID:    0,
	}
}

func (clerk *Clerk) Get(key string) string {
	args := GetArgs{Key: key}

	for {
		var reply GetReply
		ok := clerk.servers[clerk.leaderID].Call("KVServer.Get", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			clerk.leaderID = (clerk.leaderID + 1) % len(clerk.servers)
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
		ClientId: clerk.clientID,
		SeqId:    clerk.seqID,
	}
	for {
		var reply PutAppendReply
		ok := clerk.servers[clerk.leaderID].Call("KVServer.PutAppend", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			clerk.leaderID = (clerk.leaderID + 1) % len(clerk.servers)
			continue
		}
		clerk.seqID++
		return
	}
}

func (clerk *Clerk) Put(key string, value string) {
	clerk.PutAppend(key, value, "Put")
}

func (clerk *Clerk) Append(key string, value string) {
	clerk.PutAppend(key, value, "Append")
}
