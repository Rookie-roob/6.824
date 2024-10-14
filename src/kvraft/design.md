* 关于gc
    garbageShards更新数据的时机，要在updateDBWithMigrateData中进行，因为此时已经在raft层中达成了要更新的数据的共识，
确保从raft层的applyCh中拿到MigrateReply类型的数据，并更新完自己的kvdata以及CidDic后，可以删除comeInShards相应的部分并设置garbageShards
    后台的tryGC协程定期去看garbageShards长度是否为0，若不为0，则需要进行gc。接下来会通过所存的configNum查询对应版本对应的
Config，得到相对应的group，向该group发送GarbageGroup的rpc，若返回OK，则删除garbageShards对应的item。
    对于GarbageGroup，由于通过rpc形式调用，非leader直接返回，判断comeOutShards中与要删除的相关的数据，如果没有相关的数据，
也直接返回，否则就要真正进行GC的逻辑，但是在这个之前要在raft层中达成共识，往raft层传入GC的op，当server层收到了raft层相对应的信息，
且信息合法（不是WrongGroup），那么rpc的reply为OK。
    这里有一个地方要注意，那就是只有当GarbageGroup的rpc返回reply OK时，才会删除garbageShards相应的部分，如果并没有删除的话，
garbageShards对应的部分会仍然存在，等待之后的tryGC进行删除。
* 关于migrate
    如何发现新版本？ 通过detectNewConfig协程发现是否有下一个版本，若发现，则传入到raft层中达成共识，这里要注意，如果有come in的shard，要先migrate进来，否则会有覆盖现象！！！因此要先有相关的判断。
    从raft层接受到new config的apply信息时，此时说明已经根据有新config出现达成了共识，接下来就需要更新server层的信息（包括serviceShards以及直接删除要migrate出去的shard，可以直接删，因为有comeOutShards进行记录）以及Migrate的相关参数，
包括comeInShards以及comeOutShards。
    prepare migrate data的流程应该与实际migrate data解耦。因为migrate data可能会失败，但是comeInShards以及comeOutShards只要还在，后台tryPullShards协程就会不断进行！所以后台还要开一个tryPullShards协程。
    tryPullShards协程根据comeInShards进行判断需不需要migrate，如果需要，则发送Migrate的rpc，收到的reply包括shard数据，CidDic，也包括ShardID，ConfigNum，并往raft传这个reply，达成实际migrate过来数据的共识。
    当看到raft层传过来MigrateReply类型的apply信息时，进行kvdata以及CidDic的更新，此时可以删除comeInShards，也可以设置serviceShards相应的部分，也要设置garbageShards去回收comeOutShards！
* 引自网上（https://www.jianshu.com/p/f5c8ab9cd577）
  如何去PULL DATA？如果我们选择让LEADER去交互，我们必须要HANDLER RAFT Leader挂掉，得有新的LEADER来负责PULL DATA。 所以在所有节点上必须得存好要问哪里去PULL DATA。如果PULL到，我们需要确保LEADER会往RAFT里发CMD（这个CMD是让节点同步数据，同时删掉那个维护的哪里去PULL DATA的地方）
而且我们必须额外开一个后台进程与循环的做这件事。不然LEADER转移过去之后，就没有人PULL DATA了。 因为PULL DATA 这件事是没有CLIENT超时重试的。
因为要后台循环去PULL DATA，我们拿到DATA后，送进RAFT，再进入到APPLY CH，需要所有的节点都可以同步这个数据。一旦同步成功，我们需要清理这个要等待的数据。这样后台线程可以少发很多无用的RPC。
同时我们在索要数据的时候也要知道往哪个REPLICA GROUP要。
  （GC）在上面的实现里，我们开了3个数据结构，一个是TO OUT，一个是COME IN，一个是MY SHARD； 第三个是固定大小的。不用考虑 第二个，我们已经再接受到DATA之后会去删除它。 唯一没有回收的就是第一个。
最NAIVE的实现是当我们把数据当做REPLY发过去的时候，就直接删掉。这是危险的。因为很有可能这个消息会丢失，被那边服务器拒绝，造成这个数据就永远不会被回收。
正确的做法是等到对方服务器，成功接收了DATA，然后删除了对应的COME IN，这个时候应该发REQUEST告诉TO OUT一方，你可以安全的把TO OUT里的这块DATA给回收了。
但是依然存在RPC会丢失的情况。和PULL的思想一样。（用一个COME IN LIST+ 后台线程，来不断重试，成功时候删除COME IN LIST内容，就不再去PULL直到有新的COME IN来。失败的话，因为COME IN 内容还在，就会自动重试，不怕网络不稳定）
那么我针对这个CASE，用相同的套路。后台GC线程+Garbage List.
具体思路就是当COME IN 的DATA收到后，我们要把这块数据标记进Garbage List。 后台GC线程发现Garbage List有内容，就会往对应的GROUP发送GC RPC。对应的GROUP清理成功后，REPLY告知。我们把Garbage List对应的内容删除。
同样我们依然只和LEADER交互，并且利用RAFT LOG，来确保所有节点都成功删除GARBAGE，再RPC回复SUCCESS
