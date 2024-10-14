package shardkv

import (
	"github.com/Rookie-roob/6.824/src/shardmaster"
	"log"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func printShardMasterShards(dic [shardmaster.NShards]int) {
	for shard, gid := range dic {
		DPrintf("shardIndex: %d\t->\tgid: %d\n", shard, gid)
	}
}
