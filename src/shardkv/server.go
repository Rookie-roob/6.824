package shardkv

// import "../shardmaster"
import (
	"github.com/Rookie-roob/6.824/src/labrpc"
	"github.com/Rookie-roob/6.824/src/shardmaster"
	"strconv"
	"time"
)
import "github.com/Rookie-roob/6.824/src/raft"
import "sync"
import "github.com/Rookie-roob/6.824/src/labgob"

const RequstTimeout = 800

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType    string
	Key       string
	Value     string
	ClientID  int64
	CommandID int
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvdata        map[string]string
	notifyChan    map[int]chan Op
	CidDic        map[int64]int
	killChan      chan bool
	persister     *raft.Persister
	smc           *shardmaster.Clerk
	latestConfig  shardmaster.Config
	comeInShards  map[int]int                       // ShardID -> Config Num，表示要进行pull的shard以及对应的config num。
	serviceShards map[int]bool                      // 可以服务的shard
	comeOutShards map[int]map[int]map[string]string // Config Num -> (shard num -> db)
	garbageShards map[int]map[int]bool              // Config Num -> Shards
}

func ifOpSame(originOp Op, newOp Op) bool {
	return originOp.OpType == newOp.OpType && originOp.ClientID == newOp.ClientID &&
		originOp.CommandID == newOp.CommandID && originOp.Key == newOp.Key
}

func sendToChan(notifyChan chan Op, op Op) {
	select {
	case <-notifyChan:
	default:
	}
	notifyChan <- op
}

func (kv *ShardKV) getNotifyChan(logIndex int) chan Op {
	_, ok := kv.notifyChan[logIndex]
	if !ok {
		kv.notifyChan[logIndex] = make(chan Op, 1)
	} // 万一这一步之后，返回之前，就直接被delete掉这个map条目，因此外面需要加锁
	return kv.notifyChan[logIndex]
}

func (kv *ShardKV) getMessage(ch chan Op, logIndex int) Op {
	select {
	case reply := <-ch:
		close(ch)
		kv.mu.Lock()
		delete(kv.notifyChan, logIndex)
		kv.mu.Unlock()
		return reply
	case <-time.After(time.Duration(RequstTimeout) * time.Millisecond):
		return Op{}
	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		OpType:    "Get",
		Key:       args.Key,
		Value:     "",
		ClientID:  args.ClientID,
		CommandID: args.CommandID,
	} // 注意这里一定要拷贝一波，不能直接传args的引用到Start中
	index, _, isLeader := kv.rf.Start(op)
	reply.Err = ErrWrongLeader
	if !isLeader {
		return
	}
	kv.mu.Lock()
	ch := kv.getNotifyChan(index) // 相当于让get chan的这一步原子
	kv.mu.Unlock()                //select前一定要先解锁！
	replyOp := kv.getMessage(ch, index)
	if ifOpSame(op, replyOp) { //这里要和原来的op进行比较，可能传上来的不是同一个！
		reply.Err = OK
		reply.Value = replyOp.Value
		return
	}
	if replyOp.OpType == ErrWrongGroup {
		reply.Err = ErrWrongGroup
		reply.Value = ""
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		OpType:    args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClientID:  args.ClientID,
		CommandID: args.CommandID,
	} // 注意这里一定要拷贝一波，不能直接传args的引用到Start中
	index, _, isLeader := kv.rf.Start(op)
	reply.Err = ErrWrongLeader
	if !isLeader {
		return
	}
	kv.mu.Lock()
	ch := kv.getNotifyChan(index) // 相当于让get chan的这一步原子
	kv.mu.Unlock()                //select前一定要先解锁！
	replyOp := kv.getMessage(ch, index)
	if ifOpSame(op, replyOp) { //这里要和原来的op进行比较，可能传上来的不是同一个！
		reply.Err = OK
		return
	}
	if replyOp.OpType == ErrWrongGroup {
		reply.Err = ErrWrongGroup
	}
}

func (kv *ShardKV) daemon(job func(), sleepTime int) {
	for {
		select {
		case <-kv.killChan:
			return
		default:
			job()
		}
		time.Sleep(time.Duration(sleepTime) * time.Millisecond)
	}
}

func (kv *ShardKV) detectNewConfig() {
	// 相关决策都是由leader发起
	_, isLeader := kv.rf.GetState() // 不能先上kv.mu的lock再执行这一步，否则可能会死锁！！！
	kv.mu.Lock()
	// 这里不能加上comeOutShards的判断，因为删除comeOutShards的逻辑在gc中，删除comeOutShards的逻辑可能会稍稍延后，因为要确保migrate完全！
	// 从逻辑上来说也不需要加上comeOutShards的判断，能不能拿到某个shard是通过serviceShards来判断的，comeOutShards是为了辅助Migrate拿数据的，要通过gc协程删除，因为要确保migrate完全！
	// 因为要migrate出去的数据会通过comeOutShards进行记录，剩下的shard仍旧可以接受服务，也就是说下面场景：
	// 先come out shard 1，下一个版本come out shard 2，其实是可以连续进行，不需要等待的。
	if !isLeader || len(kv.comeInShards) > 0 { // re-config要一个个进行，否则可能丢失中间的更新。需要转移的SHARD还没做完。不要立刻去拿下一个CONFIG。
		kv.mu.Unlock()
		return
	}
	// 新配置如果要拉取一定要是目前配置的下一个，不能直接拉最新的
	// 如果拉最新的，那么不能group的配置版本很可能差距就会很大，整体配置直接就乱套了
	// 宏观上来看，确实每个group的配置版本可能不一样，但是最多相差1（按照按顺序一个一个连续拉取新配置）。这也能保证配置变更时需要的分片迁移的数据一定能找到。方便写删shard的部分，更方便判断。
	nextCfgIndex := kv.latestConfig.Num + 1
	nextCfg := kv.smc.Query(nextCfgIndex)
	kv.mu.Unlock()
	if nextCfg.Num == nextCfgIndex { // 说明存在更新的config，拉取大一个版本的
		kv.rf.Start(nextCfg) // 需要通过raft达成pull new config的共识
	}

}

func (kv *ShardKV) prepareInAndOutShards(newConfig shardmaster.Config) {
	// 每个server都会进行，更新自己相关的状态
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if newConfig.Num <= kv.latestConfig.Num {
		return
	}
	oldConfig, outShards := kv.latestConfig, kv.serviceShards
	kv.latestConfig = newConfig
	kv.serviceShards = make(map[int]bool)
	/*
		DPrintf("newConfig num: %v", newConfig.Num)
		printShardMasterShards(newConfig.Shards)
		DPrintf("oldConfig num: %v", oldConfig.Num)
		printShardMasterShards(oldConfig.Shards)
	*/
	for shardIndex, loopGid := range newConfig.Shards {
		if loopGid != kv.gid {
			continue
		}
		if _, ok := outShards[shardIndex]; ok || oldConfig.Num == 0 { // Ques : 为什么初始时算在这个情况中 Ans: 因为一开始初始情况下所有shard都由gid为0的group管，因此要先把他们都移出来（开始service时合法的gid都大于0！test中从101开始递增）
			kv.serviceShards[shardIndex] = true // 第一个版本要归到这个情况中，与后面的逻辑不太一样，相当于直接设置serviceshards就行，comeinshards和comeoutshards的大小都为0
			delete(outShards, shardIndex)
		} else {
			kv.comeInShards[shardIndex] = oldConfig.Num // 记录的版本是此时的老版本
		}
	}
	if len(outShards) > 0 {
		kv.comeOutShards[oldConfig.Num] = make(map[int]map[string]string) // 记录的版本是此时的老版本
		for shardIndex := range outShards {
			outDB := make(map[string]string)
			for k, v := range kv.kvdata {
				if key2shard(k) == shardIndex {
					outDB[k] = v
					delete(kv.kvdata, k) // 在db中删除数据的时机在这里，往comeOutShards中加入删除的db数据
				}
			}
			kv.comeOutShards[oldConfig.Num][shardIndex] = outDB
		}
	}
}

func (kv *ShardKV) updateDBWithMigrateData(migrateReply MigrateReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if migrateReply.ConfigNum != kv.latestConfig.Num-1 {
		return
	}
	delete(kv.comeInShards, migrateReply.ShardID)
	if _, ok := kv.serviceShards[migrateReply.ShardID]; !ok { // 确保不要用旧版本的覆盖此时新版本的数据，新拉取的应该是本来不在本集合中的，不然会造成覆盖，且拉取过来的是旧版本下另一个group的数据
		kv.serviceShards[migrateReply.ShardID] = true
		for k, v := range migrateReply.KVData {
			kv.kvdata[k] = v
		}
		for k, v := range migrateReply.CidDic {
			kv.CidDic[k] = max(kv.CidDic[k], v)
		}
		// 确定在raft达成共识，且传到了状态机后，进行gc
		if _, ok := kv.garbageShards[migrateReply.ConfigNum]; !ok {
			kv.garbageShards[migrateReply.ConfigNum] = make(map[int]bool)
		}
		kv.garbageShards[migrateReply.ConfigNum][migrateReply.ShardID] = true
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	select {
	case <-kv.killChan:
	default:
	}
	kv.killChan <- true
}

func (kv *ShardKV) processApplyFromRaft() {
	for {
		select {
		case <-kv.killChan:
			return
		case applyData := <-kv.applyCh:
			if applyData.CommandValid {
				if newCfg, ok := applyData.Command.(shardmaster.Config); ok {
					kv.prepareInAndOutShards(newCfg)
				} else if migrateReply, ok := applyData.Command.(MigrateReply); ok {
					kv.updateDBWithMigrateData(migrateReply)
				} else {
					op := applyData.Command.(Op)
					if op.OpType == "GC" {
						cfgNum, _ := strconv.Atoi(op.Key)
						kv.doGC(cfgNum, op.CommandID)
					} else {
						kv.doDataOperation(&op)
					}
					kv.mu.Lock()
					ch := kv.getNotifyChan(applyData.CommandIndex) // 一定要注意这里是log index，不要写错了！！！
					kv.mu.Unlock()
					sendToChan(ch, op)
				}
				ifSnapshot := kv.checkSnapshot()
				if ifSnapshot {
					go kv.rf.DoSnapshot(applyData.CommandIndex, kv.encodeSnapshot()) //注意这个操作每个server都是有可能主动snapshot的！！！
				}
			} else {
				kv.decodeKVSnapshot(applyData.Snapshot)
			}
		}
	}
}

func (kv *ShardKV) doDataOperation(op *Op) {
	shard := key2shard(op.Key)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.serviceShards[shard]; !ok {
		op.OpType = ErrWrongGroup // 在这里判断ErrWrongGroup的逻辑，从raft层中接收到apply的信息再进行判断
	} else {
		maxCommandID, found := kv.CidDic[op.ClientID]
		if op.OpType != "Get" && (!found || maxCommandID < op.CommandID) {
			if op.OpType == "Put" {
				kv.kvdata[op.Key] = op.Value
			} else if op.OpType == "Append" {
				kv.kvdata[op.Key] += op.Value
			}
			kv.CidDic[op.ClientID] = op.CommandID // 更新为状态机最新更新的op的CommandID，别忘记！！！
		}
		if op.OpType == "Get" {
			op.Value = kv.kvdata[op.Key]
		}
	}
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(MigrateArgs{})
	labgob.Register(MigrateReply{})
	labgob.Register(shardmaster.Config{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters
	kv.persister = persister

	// Your initialization code here.
	kv.kvdata = make(map[string]string)
	kv.notifyChan = make(map[int]chan Op)
	kv.CidDic = make(map[int64]int)
	kv.latestConfig = shardmaster.Config{}
	kv.comeInShards = make(map[int]int)
	kv.comeOutShards = make(map[int]map[int]map[string]string)
	kv.serviceShards = make(map[int]bool)
	kv.garbageShards = make(map[int]map[int]bool)

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)
	kv.smc = shardmaster.MakeClerk(kv.masters)

	kv.decodeKVSnapshot(kv.persister.ReadSnapshot()) // 别忘记在StartServer这里恢复快照！

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.killChan = make(chan bool, 1)
	go kv.processApplyFromRaft()
	go kv.daemon(kv.detectNewConfig, 50)
	go kv.daemon(kv.tryPullShards, 70)
	go kv.daemon(kv.tryGC, 100)
	return kv
}
