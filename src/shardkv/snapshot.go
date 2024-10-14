package shardkv

import (
	"bytes"
	"fmt"
	"github.com/Rookie-roob/6.824/src/labgob"
	"github.com/Rookie-roob/6.824/src/shardmaster"
)

func (kv *ShardKV) checkSnapshot() bool {
	if kv.maxraftstate == -1 {
		return false
	}
	return float64(kv.persister.RaftStateSize())/float64(kv.maxraftstate) > 0.95 // 这一步去获取raft层大小时，不要去获取rf的lock，不然容易死锁！！！！
}

func (kv *ShardKV) encodeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	kv.mu.Lock()
	// 不用存gid！！！每个server属于哪个group是固定的
	e.Encode(kv.CidDic)
	e.Encode(kv.kvdata)
	e.Encode(kv.latestConfig)
	e.Encode(kv.serviceShards)
	e.Encode(kv.comeOutShards)
	e.Encode(kv.comeInShards)
	e.Encode(kv.garbageShards)
	kv.mu.Unlock()
	data := w.Bytes()
	return data
}

func (kv *ShardKV) decodeKVSnapshot(snapshotKVData []byte) {
	if snapshotKVData == nil || len(snapshotKVData) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(snapshotKVData)
	d := labgob.NewDecoder(r)
	var commandMap map[int64]int
	var kvdata map[string]string
	var latestConfig shardmaster.Config
	var serviceShards map[int]bool
	var comeOutShards map[int]map[int]map[string]string
	var comeInShards map[int]int
	var garbageShards map[int]map[int]bool
	if d.Decode(&commandMap) != nil ||
		d.Decode(&kvdata) != nil ||
		d.Decode(&latestConfig) != nil ||
		d.Decode(&serviceShards) != nil ||
		d.Decode(&comeOutShards) != nil ||
		d.Decode(&comeInShards) != nil ||
		d.Decode(&garbageShards) != nil {
		fmt.Println("decode error!!!")
	} else {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		kv.CidDic = commandMap
		kv.kvdata = kvdata
		kv.latestConfig = latestConfig
		kv.serviceShards = serviceShards
		kv.comeOutShards = comeOutShards
		kv.comeInShards = comeInShards
		kv.garbageShards = garbageShards
	}
}
