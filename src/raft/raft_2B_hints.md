* Run git pull to get the latest lab software.

* Your first goal should be to **pass TestBasicAgree2B()**. Start by implementing Start(), then write the code to send and receive new log entries via AppendEntries RPCs, following Figure 2.

* You will need to implement the election restriction (section 5.4.1 in the paper).

* One way to fail to reach agreement in the early Lab 2B tests is to hold repeated elections even though the leader is alive. Look for bugs in election timer management, or not sending out heartbeats immediately after winning an election.

* Your code may have loops that repeatedly check for certain events. Don't have these loops execute continuously without pausing, since that will slow your implementation enough that it fails tests. Use Go's condition variables, or insert a time.Sleep(10 * time.Millisecond) in each loop iteration.

* Do yourself a favor for future labs and write (or re-write) code that's clean and clear. For ideas, you can re-visit our structure, locking, and guide pages.