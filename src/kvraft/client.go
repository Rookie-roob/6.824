package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"github.com/Rookie-roob/6.824/src/labrpc"
)

const RetryInterval = 100 * time.Millisecond

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lastLeaderID  int
	lastCommandID int
	clientID      int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientID = nrand()
	ck.lastLeaderID = 0
	ck.lastCommandID = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
// 要求中说：
// It’s OK to assume that a client will make only one call into a Clerk at a time.
// 所以这里的逻辑是不用上锁的！
func (ck *Clerk) Get(key string) string {
	getArgs := GetArgs{
		Key:       key,
		CommandID: ck.lastCommandID,
		ClientID:  ck.clientID,
	}
	i := ck.lastLeaderID
	for {
		getReply := GetReply{}
		ok := ck.servers[i].Call("KVServer.Get", &getArgs, &getReply)
		if !ok || getReply.WrongLeader {
			i = (i + 1) % (len(ck.servers))
			time.Sleep(RetryInterval)
			continue
		}
		ck.lastLeaderID = i
		return getReply.Value
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	putAppendArgs := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		CommandID: ck.lastCommandID,
		ClientID:  ck.clientID,
	}
	i := ck.lastLeaderID
	ck.lastCommandID++
	for {
		putAppendReply := PutAppendReply{}
		ok := ck.servers[i].Call("KVServer.PutAppend", &putAppendArgs, &putAppendReply)
		if !ok || putAppendReply.WrongLeader {
			i = (i + 1) % (len(ck.servers))
			time.Sleep(RetryInterval)
			continue
		}
		ck.lastLeaderID = i
		return
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
