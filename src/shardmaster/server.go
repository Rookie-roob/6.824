package shardmaster

import (
	"github.com/Rookie-roob/6.824/src/raft"
	"log"
	"time"
)
import "github.com/Rookie-roob/6.824/src/labrpc"
import "sync"
import "github.com/Rookie-roob/6.824/src/labgob"

const RequstTimeout = 600

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	closeChan  chan struct{}
	CidDic     map[int64]int
	notifyChan map[int]chan Op //log index为key

	configs []Config // indexed by config num
}

type Op struct {
	// Your data here.
	OpType    string
	OpArgs    interface{}
	ClientID  int64
	CommandID int
}

func (sm *ShardMaster) getNotifyChan(logIndex int) chan Op {
	_, ok := sm.notifyChan[logIndex]
	if !ok {
		sm.notifyChan[logIndex] = make(chan Op, 1)
	} // 万一这一步之后，返回之前，就直接被delete掉这个map条目，因此外面需要加锁
	return sm.notifyChan[logIndex]
}

func (sm *ShardMaster) getMessage(ch chan Op, logIndex int) Op {
	select {
	case reply := <-ch:
		close(ch)
		sm.mu.Lock()
		delete(sm.notifyChan, logIndex)
		sm.mu.Unlock()
		return reply
	case <-time.After(time.Duration(RequstTimeout) * time.Millisecond):
		return Op{}
	}
}

func ifOpSame(originOp Op, newOp Op) bool {
	return originOp.OpType == newOp.OpType && originOp.ClientID == newOp.ClientID &&
		originOp.CommandID == newOp.CommandID
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op{
		OpType:    "Join",
		OpArgs:    *args,
		ClientID:  args.ClientID,
		CommandID: args.CommandID,
	}
	index, _, isLeader := sm.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	sm.mu.Lock()
	ch := sm.getNotifyChan(index)
	sm.mu.Unlock()
	replyOp := sm.getMessage(ch, index)
	if ifOpSame(replyOp, op) {
		reply.WrongLeader = false
	} else {
		reply.WrongLeader = true
	}
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op{
		OpType:    "Leave",
		OpArgs:    *args,
		ClientID:  args.ClientID,
		CommandID: args.CommandID,
	}
	index, _, isLeader := sm.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	sm.mu.Lock()
	ch := sm.getNotifyChan(index)
	sm.mu.Unlock()
	replyOp := sm.getMessage(ch, index)
	if ifOpSame(replyOp, op) {
		reply.WrongLeader = false
	} else {
		reply.WrongLeader = true
	}
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op{
		OpType:    "Move",
		OpArgs:    *args,
		ClientID:  args.ClientID,
		CommandID: args.CommandID,
	}
	index, _, isLeader := sm.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	sm.mu.Lock()
	ch := sm.getNotifyChan(index)
	sm.mu.Unlock()
	replyOp := sm.getMessage(ch, index)
	if ifOpSame(replyOp, op) {
		reply.WrongLeader = false
	} else {
		reply.WrongLeader = true
	}
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op{
		OpType:    "Query",
		OpArgs:    *args,
		ClientID:  args.ClientID,
		CommandID: args.CommandID,
	}
	index, _, isLeader := sm.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	sm.mu.Lock()
	ch := sm.getNotifyChan(index)
	sm.mu.Unlock()
	replyOp := sm.getMessage(ch, index)
	if ifOpSame(replyOp, op) {
		reply.WrongLeader = false
		sm.mu.Lock()
		defer sm.mu.Unlock()
		if args.Num >= 0 && args.Num < len(sm.configs) {
			reply.Config = sm.configs[args.Num]
		} else {
			reply.Config = sm.configs[len(sm.configs)-1]
		}
	} else {
		reply.WrongLeader = true
	}
}

// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
	close(sm.closeChan)
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

func sendToChan(notifyChan chan Op, op Op) {
	select {
	case <-notifyChan:
	default:
	}
	notifyChan <- op
}

func (sm *ShardMaster) processApplyFromRaft() {
	for {
		select {
		case <-sm.closeChan:
			return
		case applyData := <-sm.applyCh:
			if applyData.CommandValid {
				op := applyData.Command.(Op)
				sm.mu.Lock()
				maxCommandID, ok := sm.CidDic[op.ClientID]
				if op.OpType != "Query" && (!ok || maxCommandID < op.CommandID) {
					sm.updateConfig(op.OpType, op.OpArgs)
					sm.CidDic[op.ClientID] = op.CommandID
				}
				ch := sm.getNotifyChan(applyData.CommandIndex)
				sm.mu.Unlock()
				sendToChan(ch, op)
			} else {
				continue
			}
		}
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(QueryArgs{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.CidDic = make(map[int64]int)
	sm.notifyChan = make(map[int]chan Op)
	sm.closeChan = make(chan struct{})

	go sm.processApplyFromRaft() // 别忘记启动这个后台协程！！！
	return sm
}

func getMaxGid(gid2shardCnt map[int][]int) int {
	maxvalue := -1
	gid := 0
	for curgid, sidlist := range gid2shardCnt {
		if len(sidlist) > maxvalue {
			maxvalue = len(sidlist)
			gid = curgid
		}
	}
	return gid
}

func getMinGid(gid2shardCnt map[int][]int) int {
	minvalue := 1<<31 - 1
	gid := 0
	for curgid, sidlist := range gid2shardCnt {
		if len(sidlist) < minvalue {
			minvalue = len(sidlist)
			gid = curgid
		}
	}
	return gid
}

func (sm *ShardMaster) rebalanced(newConfig *Config, opType string, gid int) {
	gid2shard := make(map[int][]int)
	for k, _ := range newConfig.Groups {
		gid2shard[k] = []int{}
	}
	for sid, curgid := range newConfig.Shards {
		gid2shard[curgid] = append(gid2shard[curgid], sid)
	}
	if opType == "Join" {
		averageShardCnt := NShards / len(newConfig.Groups) // 是一种归纳法的做法，没有balance之前是平衡的，进行balance操作，则是将最多的gid中的shard分给新来的group，要分多少，显然是平均数。
		for i := 0; i < averageShardCnt; i++ {
			srcgid := getMaxGid(gid2shard)
			newConfig.Shards[gid2shard[srcgid][0]] = gid
			gid2shard[srcgid] = gid2shard[srcgid][1:]
		}
	} else if opType == "Leave" {
		_, ok := gid2shard[gid]
		if !ok {
			delete(newConfig.Groups, gid)
			return // 可能这个gid本来就不存在或者所包含的shard数量为0，那么直接返回
		}
		delete(newConfig.Groups, gid)
		movelist := make([]int, len(gid2shard[gid]), len(gid2shard[gid]))
		copy(movelist, gid2shard[gid])
		delete(gid2shard, gid)
		if len(newConfig.Groups) == 0 {
			newConfig.Shards = [NShards]int{}
			return
		}
		for _, shard := range movelist {
			dstgid := getMinGid(gid2shard)
			newConfig.Shards[shard] = dstgid
			gid2shard[dstgid] = append(gid2shard[dstgid], shard)
		}
	} else {
		log.Fatal("invalid rebalanced!!!")
	}
}

func (sm *ShardMaster) updateConfig(opType string, opArgs interface{}) {
	newConfig := sm.copyLatestConfig()
	if opType == "Join" { // Join情况为新加入group
		joinArgs := opArgs.(JoinArgs)
		for gid, v := range joinArgs.Servers {
			newConfig.Groups[gid] = v
			sm.rebalanced(&newConfig, opType, gid)
		}
	} else if opType == "Leave" {
		leaveArgs := opArgs.(LeaveArgs)
		for _, gid := range leaveArgs.GIDs {
			sm.rebalanced(&newConfig, opType, gid)
		}
	} else if opType == "Move" {
		moveArgs := opArgs.(MoveArgs)
		if _, ok := newConfig.Groups[moveArgs.GID]; ok {
			newConfig.Shards[moveArgs.Shard] = moveArgs.GID
		} else {
			return
		}
	} else {
		log.Fatal("invalid updateConfig!!!")
	}
	sm.configs = append(sm.configs, newConfig)
}
func (sm *ShardMaster) copyLatestConfig() Config {
	latestConfig := sm.configs[len(sm.configs)-1]
	newConfig := Config{
		Num:    latestConfig.Num + 1,
		Shards: latestConfig.Shards,    // go中的数组拷贝是深拷贝
		Groups: make(map[int][]string), // go中的map以及slice为浅拷贝
	}
	for k, v := range latestConfig.Groups {
		newConfig.Groups[k] = append([]string{}, v...)
	}
	return newConfig
}
