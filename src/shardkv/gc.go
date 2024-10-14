package shardkv

import (
	"github.com/Rookie-roob/6.824/src/shardmaster"
	"strconv"
	"sync"
)

func (kv *ShardKV) tryGC() {
	_, isLeader := kv.rf.GetState()
	kv.mu.Lock()
	if !isLeader || len(kv.garbageShards) == 0 {
		kv.mu.Unlock()
		return
	}
	var waitGroup sync.WaitGroup
	for cfgNum, shards := range kv.garbageShards {
		for shard := range shards {
			waitGroup.Add(1)
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
					if ok := srv.Call("ShardKV.GarbageGroup", &args, &reply); ok && reply.Err == OK {
						kv.mu.Lock()
						delete(kv.garbageShards[cfgNum], shard)
						if len(kv.garbageShards[cfgNum]) == 0 {
							delete(kv.garbageShards, cfgNum)
						}
						kv.mu.Unlock()
					}
				}
			}(shard, kv.smc.Query(cfgNum))
		}
	}
	kv.mu.Unlock()
	waitGroup.Wait()
}

func (kv *ShardKV) GarbageGroup(args *MigrateArgs, reply *MigrateReply) {
	reply.Err = ErrWrongLeader
	if _, isLeader := kv.rf.GetState(); !isLeader {
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.comeOutShards[args.ConfigNum]; !ok {
		return
	}
	if _, ok := kv.comeOutShards[args.ConfigNum][args.ShardID]; !ok {
		return
	}
	garbageOp := Op{
		OpType:    "GC",
		Key:       strconv.Itoa(args.ConfigNum),
		Value:     "",
		ClientID:  nrand(),
		CommandID: args.ShardID,
	}
	kv.mu.Unlock()
	index, _, isLeader := kv.rf.Start(garbageOp)
	if isLeader {
		kv.mu.Lock()
		ch := kv.getNotifyChan(index)
		kv.mu.Unlock()
		op := kv.getMessage(ch, index)
		if ifOpSame(garbageOp, op) {
			reply.Err = OK
		}
		if op.OpType == ErrWrongGroup {
			reply.Err = ErrWrongGroup
		}
	}
	kv.mu.Lock()
}

func (kv *ShardKV) doGC(cfgNum int, shard int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.comeOutShards[cfgNum]; !ok {
		return
	}
	delete(kv.comeOutShards[cfgNum], shard)
	if len(kv.comeOutShards[cfgNum]) == 0 {
		delete(kv.comeOutShards, cfgNum)
	}
}
