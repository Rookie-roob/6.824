* You can't easily run your Raft implementation directly; instead you should run it by way of the tester, i.e. go test -run 2A.

* Follow the paper's Figure 2. At this point you care about sending and receiving RequestVote RPCs, the Rules for Servers that relate to elections, and the State related to leader election.

* Add the Figure 2 state for leader election to the Raft struct in raft.go. You'll also need to define **a struct to hold information about each log entry**.

* Fill in the RequestVoteArgs and RequestVoteReply structs. **Modify Make() to create a background goroutine that will kick off leader election periodically by sending out RequestVote RPCs when it hasn't heard from another peer for a while.** This way a peer will learn who is the leader, if there is already a leader, or become the leader itself. Implement the RequestVote() RPC handler so that servers will vote for one another.

* To implement heartbeats, define an AppendEntries RPC struct (though you may not need all the arguments yet), and have the leader send them out periodically. Write an AppendEntries RPC handler method that resets the election timeout so that other servers don't step forward as leaders when one has already been elected.

* Make sure the election timeouts in different peers don't always fire at the same time, or else all peers will vote only for themselves and no one will become the leader.

* The tester requires that **the leader send heartbeat RPCs no more than ten times per second**.

* The tester requires your Raft to **elect a new leader within five seconds of the failure of the old leader (if a majority of peers can still communicate)**. Remember, however, that leader election may require multiple rounds in case of a split vote (which can happen if packets are lost or if candidates unluckily choose the same random backoff times). You must pick election timeouts (and thus heartbeat intervals) that are short enough that it's very likely that an election will complete in less than five seconds even if it requires multiple rounds.

* The paper's Section 5.2 mentions election timeouts in the range of 150 to 300 milliseconds. Such a range only makes sense if the leader sends heartbeats considerably more often than once per 150 milliseconds. Because the tester limits you to 10 heartbeats per second, you will have to use an election timeout larger than the paper's 150 to 300 milliseconds, but not too large, because then you may fail to elect a leader within five seconds.

* You may find **Go's rand** useful.

* You'll need to write code that takes actions periodically or after delays in time. The easiest way to do this is to **create a goroutine with a loop that calls time.Sleep(). Don't use Go's time.Timer or time.Ticker**, which are difficult to use correctly.

* Read this advice about locking and structure.

* If your code has trouble passing the tests, read the paper's Figure 2 again; the full logic for leader election is spread over multiple parts of the figure.

* Don't forget to **implement GetState()**.

* The tester calls your Raft's rf.Kill() when it is permanently shutting down an instance. You can check whether Kill() has been called using rf.killed(). You may want to do this in all loops, to avoid having dead Raft instances print confusing messages.

* A good way to debug your code is to insert print statements when a peer sends or receives a message, and collect the output in a file with go test -run 2A > out. Then, by studying the trace of messages in the out file, you can identify where your implementation deviates from the desired protocol. You might find DPrintf in util.go useful to turn printing on and off as you debug different problems.

* Go RPC sends only **struct fields whose names start with capital letters**. Sub-structures must also have capitalized field names (e.g. fields of log records in an array). The labgob package will warn you about this; don't ignore the warnings.

* Check your code with go test -race, and fix any races it reports.