package kvraft

import "time"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
)

type Err string

const ClientRequestTimeout = 500 * time.Millisecond

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

// Put or Append
type PutAppendArgs struct {
	Key      string
	Value    string
	Op       string // "Put" or "Append"
	ClientId int64
	SeqId    int64
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type Op struct {
	Key           string
	Value         string
	OperationType OperationType
	ClientID      int64
	SeqID         int64
}

type OperationType uint8

const (
	OpGet = iota
	OpPut
	OpAppend
)

func getOperationType(s string) OperationType {
	switch s {
	case "Put":
		return OpPut
	case "Append":
		return OpAppend
	case "Get":
		return OpGet
	}
	panic("invalid operation type")
}

type OpReply struct {
	Value string
	Err   Err
}

type LastOperationInfo struct {
	SeqID int64
	Reply *OpReply
}
