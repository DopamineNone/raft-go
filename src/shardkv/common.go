package shardkv

import "time"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongConfig = "ErrWrongConfig"
	ErrNotReady    = "ErrNotReady"
)

type Err string

const (
	ClientRequestTimeout = 500 * time.Millisecond
	FetchConfigInterval  = 100 * time.Millisecond
	ShardMigrateInterval = 50 * time.Millisecond
	ShardGCInterval
)

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
	ClientId      int64
	SeqId         int64
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

var (
	OKReply = &OpReply{Err: OK}
)

type LastOperationInfo struct {
	SeqID int64
	Reply *OpReply
}

func (info *LastOperationInfo) copy() *LastOperationInfo {
	return &LastOperationInfo{
		SeqID: info.SeqID,
		Reply: &OpReply{
			Value: info.Reply.Value,
			Err:   info.Reply.Err,
		},
	}
}

type RaftCommandType uint8

const (
	ClientOperation RaftCommandType = iota
	ConfigurationChange
	ShardMigration
	ShardGC
)

type RaftCommand struct {
	CommandType RaftCommandType
	Data        interface{}
}

type ShardStatus uint8

const (
	Normal ShardStatus = iota
	MoveIn
	MoveOut
	GC
)

type ShardOperationArgs struct {
	ConfigNum int
	ShardIds  []int
}

type ShardOperationReply struct {
	Err       Err
	ConfigNum int
	ShardData map[int]map[string]string
	DupTable  map[int64]*LastOperationInfo
}
