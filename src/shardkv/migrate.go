package shardkv

import (
	"github.com/Rookie-roob/6.824/src/shardmaster"
	"sync"
)

type MigrateArgs struct {
	ShardID   int
	ConfigNum int
}

type MigrateReply struct {
	Err       Err
	ConfigNum int
	ShardID   int
	KVData    map[string]string
	CidDic    map[int64]int
}

func (kv *ShardKV) Migrate(args *MigrateArgs, reply *MigrateReply) {
	reply.Err = ErrWrongLeader // 先设置一个默认返回值
	reply.ShardID = args.ShardID
	reply.ConfigNum = args.ConfigNum
	if _, isLeader := kv.rf.GetState(); !isLeader {
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Err = ErrWrongGroup
	if args.ConfigNum >= kv.latestConfig.Num { // 被拉取的group的config也已经到了新的config状态
		return
	}
	reply.Err = OK
	reply.KVData, reply.CidDic = kv.copyKVDataAndCidDic(args.ShardID, args.ConfigNum)
}

func (kv *ShardKV) copyKVDataAndCidDic(shardID int, configNum int) (map[string]string, map[int64]int) {
	kvData := make(map[string]string)
	cidDic := make(map[int64]int)
	for k, v := range kv.comeOutShards[configNum][shardID] {
		kvData[k] = v
	}
	for k, v := range kv.CidDic {
		cidDic[k] = v
	}
	return kvData, cidDic
}

func (kv *ShardKV) tryPullShards() {
	_, isLeader := kv.rf.GetState() // 不能先上kv.mu的lock再执行这一步，否则可能会死锁！！！
	kv.mu.Lock()
	if !isLeader || len(kv.comeInShards) == 0 { // 若len(kv.comeInShards) == 0说明没有要进来的shards直接返回
		kv.mu.Unlock()
		return
	}
	var waitGroup sync.WaitGroup
	for shardIndex, configIndex := range kv.comeInShards {
		waitGroup.Add(1)
		// 该实现中对于每一个shard都会调用一次rpc
		go func(shardIndex int, cfg shardmaster.Config) {
			defer waitGroup.Done()
			args := MigrateArgs{
				ShardID:   shardIndex,
				ConfigNum: cfg.Num,
			}
			gid := cfg.Shards[shardIndex]
			for _, server := range cfg.Groups[gid] {
				srv := kv.make_end(server)
				reply := MigrateReply{}
				if ok := srv.Call("ShardKV.Migrate", &args, &reply); ok && reply.Err == OK { // migrate失败的话也没有关系，因为没有成功删除comeInShards，下次tryPullShards协程会进行重试。comeInShards删除的逻辑在MigrateReply从raft apply到server层。
					kv.rf.Start(reply)
				}
			}
		}(shardIndex, kv.smc.Query(configIndex))
	}
	kv.mu.Unlock()
	waitGroup.Wait()
}
