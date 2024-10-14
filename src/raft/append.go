package raft

type AppendEntriesArgs struct {
	Term               int
	LeaderId           int
	PrevLogIndex       int
	PrevLogTerm        int
	Entries            []LogEntry
	LeaderCommit       int
	LeaderLogLastIndex int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Success = false
	reply.Term = rf.currentTerm
	reply.ConflictIndex = -1
	reply.Term = 0
	reply.ConflictTerm = -1
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.beFollower(args.Term)
	}
	sendToChan(rf.appendCh)
	currentPrevLogTerm := -1
	currentLogSize := len(rf.logs) + rf.lastIncludedIndex
	if args.PrevLogIndex >= rf.lastIncludedIndex && args.PrevLogIndex < currentLogSize {
		currentPrevLogTerm = rf.logs[args.PrevLogIndex-rf.lastIncludedIndex].Term
	}
	if currentPrevLogTerm != args.PrevLogTerm {
		reply.ConflictIndex = currentLogSize
		if currentPrevLogTerm == -1 {
			// 这种情况下是receiver的日志不够长，对应index处为空
		} else {
			reply.ConflictTerm = currentPrevLogTerm
			left := rf.lastIncludedIndex // 其实不应该是commitIndex，因为可能该server过于落后，该server的commitIndex过于落后
			// 而且多复制一点日志其实没有太多影响
			right := args.PrevLogIndex
			for left < right {
				mid := (left + right) >> 1
				if rf.logs[mid-rf.lastIncludedIndex].Term >= reply.ConflictTerm {
					right = mid
				} else {
					left = mid + 1
				}
			}
			reply.ConflictIndex = left
		}
		return
	}
	writeIndex := args.PrevLogIndex
	for i := 0; i < len(args.Entries); i++ {
		writeIndex++
		if writeIndex < currentLogSize {
			if rf.logs[writeIndex-rf.lastIncludedIndex].Term == args.Entries[i].Term {
				continue
			} else {
				rf.logs = rf.logs[:writeIndex-rf.lastIncludedIndex]
			}
		}
		rf.logs = append(rf.logs, args.Entries[i:]...)
		rf.persist()
		break
	}
	// 更新commitIndex
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = Min(args.LeaderCommit, rf.lastIncludedIndex+len(rf.logs)-1)
		rf.Commit()
	}
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) broadcastHeartbeat() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(peer int) {
			for {
				rf.mu.Lock()
				if rf.state != LEADER {
					rf.mu.Unlock()
					return
				}
				if rf.nextIndex[peer]-1 < rf.lastIncludedIndex {
					rf.sendSnapshot(peer)
					return
				}
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: rf.nextIndex[peer] - 1,
					PrevLogTerm:  -1,
					Entries:      append(make([]LogEntry, 0), rf.logs[rf.nextIndex[peer]-rf.lastIncludedIndex:]...),
					LeaderCommit: rf.commitIndex,
				}
				if args.PrevLogIndex >= rf.lastIncludedIndex {
					args.PrevLogTerm = rf.logs[args.PrevLogIndex-rf.lastIncludedIndex].Term
				}
				rf.mu.Unlock()

				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(peer, &args, &reply)
				rf.mu.Lock()
				if !ok || rf.state != LEADER || rf.currentTerm != args.Term {
					rf.mu.Unlock()
					return
				}
				if reply.Term > rf.currentTerm {
					rf.beFollower(reply.Term)
					rf.mu.Unlock()
					return
				}
				if reply.Success {
					rf.updateMatchIndex(peer, args.PrevLogIndex+len(args.Entries))
					rf.mu.Unlock()
					return
				} else {
					index := reply.ConflictIndex
					if reply.ConflictTerm != -1 {
						curLogLen := rf.lastIncludedIndex + len(rf.logs)
						for i := rf.lastIncludedIndex; i < curLogLen; i++ {
							if rf.logs[i-rf.lastIncludedIndex].Term != reply.ConflictTerm {
								continue
							}
							for i < curLogLen && rf.logs[i-rf.lastIncludedIndex].Term == reply.ConflictTerm {
								i++
							}
							index = i
						}
					}
					rf.nextIndex[peer] = Min(rf.lastIncludedIndex+len(rf.logs), index)
					rf.mu.Unlock()
				}
			}
		}(i)
	}
}
