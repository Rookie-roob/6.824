package shardmaster

//
// Shardmaster clerk.
//

import "github.com/Rookie-roob/6.824/src/labrpc"
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
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
	// Your code here.
	ck.lastLeaderID = 0
	ck.lastCommandID = 0
	ck.clientID = nrand()
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.ClientID = ck.clientID
	args.CommandID = ck.lastCommandID
	args.Num = num
	for {
		// try each known server.
		i := ck.lastLeaderID
		for j := 0; j < len(ck.servers); j++ {
			srv := ck.servers[(i+j)%(len(ck.servers))]
			var reply QueryReply
			ok := srv.Call("ShardMaster.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.lastLeaderID = (i + j) % (len(ck.servers))
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	args.ClientID = ck.clientID
	args.CommandID = ck.lastCommandID
	ck.lastCommandID++
	for {
		// try each known server.
		i := ck.lastLeaderID
		for j := 0; j < len(ck.servers); j++ {
			srv := ck.servers[(i+j)%(len(ck.servers))]
			var reply JoinReply
			ok := srv.Call("ShardMaster.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.lastLeaderID = (i + j) % (len(ck.servers))
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	args.ClientID = ck.clientID
	args.CommandID = ck.lastCommandID
	ck.lastCommandID++
	for {
		// try each known server.
		i := ck.lastLeaderID
		for j := 0; j < len(ck.servers); j++ {
			srv := ck.servers[(i+j)%(len(ck.servers))]
			var reply LeaveReply
			ok := srv.Call("ShardMaster.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.lastLeaderID = (i + j) % (len(ck.servers))
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	args.ClientID = ck.clientID
	args.CommandID = ck.lastCommandID
	ck.lastCommandID++
	for {
		// try each known server.
		i := ck.lastLeaderID
		for j := 0; j < len(ck.servers); j++ {
			srv := ck.servers[(i+j)%(len(ck.servers))]
			var reply MoveReply
			ok := srv.Call("ShardMaster.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.lastLeaderID = (i + j) % (len(ck.servers))
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
