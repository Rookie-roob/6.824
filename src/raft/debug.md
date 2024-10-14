* lab2 踩坑点
* 2a ： 发起election投票时，一定不要忘记将args初始化正确！
* 2b：最大的坑，在于调用rpc时，如果对方peer已经退出，一定要及时停止，不然会占用cpu的时间，导致test不过！
* 以及，当nextindex不匹配时，下一个nextindex的寻找过程可以使用论文中提到过的加速方式进行，即返回conflict term以及conflict index 的方式。
* 另外，在request vote以及append entries返回值最好改成相应的状态，这样能更好地处理结果。
* 2C：注意读取持久化记录时，对于log的读取，要是引用，不能直接传进去（切片和切片引用作为函数参数的区别）
* （2b稀里糊涂过了，2c里就没过）conflict index和conflict term的确定流程，当有日志且日志不匹配时，conflict term为当前receiver的log中term，conflict index为receiver的log中最左边满足日志term等于conlict term的index。
* apply将日志条目上传到applyCh中，以及appendEntries中对于rf.logs的修改操作，这两个是要进行同步的，不要忘记上锁同步！！！
* 要注意leader修改commitIndex的规则：
* If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N
* 在我们的实现中，是在sendAppendEntries中进行commitIndex的修改，不要简单的等于日志末尾的长度，而是要按照上面的规则进行修改！