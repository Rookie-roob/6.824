* 对于snapshot，要思考持久化的是哪些状态，一开始忘记持久化lastConfig了，这个是肯定要持久化的，能够知道目前所处shardmaster对应的版本，而gid是不需要持久化的，因为每个server属于哪个group是固定的！
* 对于server端put，append操作在状态机成功应用后，别忘记更新CidDic，更新为该Op的CommandIndex，即kv.CidDic[op.ClientID] = op.CommandID。
* 从raft层收到MigratreReply类型的信息时，此时真正更新新版本的shard数据，要注意更新CidDic，要取max，因为CidDic是ClientID -> CommandID，极有可能MigrateReply中的大，也有可能自己的CidDic中的大（Client往该gid发送了更新的Command）。
* 想象一个场景，2个raft group，在某个config版本raft group 2向raft group 1migrate一个shard a，raft group 1在之后涉及多次重启，且这个时期client也没有向raft group 1发送请求，此时raft group 1没有commit这个migrate的log，但此时也没有请求，
因此这个shard的migrate一直被卡着，此时有client发送了对应该shard的请求，此时最新版本该shard所在的位置在raft group 3中，依然不会向raft group 1发送请求，此时就会造成活锁。（在网上看到了关于活锁的现象，想象了一下这个场景，但是在我们的这个结构中，
不会有这个现象，因为comeInShards删除的条件是收到了raft层apply的信息，否则tryPullShards协程会不断往raft层打日志的，所以在这个架构中不用担心这个问题）