package raft

import (
	"bytes"

	"github.com/Rookie-roob/6.824/src/labgob"
)

func (rf *Raft) persistStateMachineAndRaftState(kvdata []byte) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data, kvdata)
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	//Offset            int
	Data []byte
	//done              bool
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.beFollower(args.Term)
	}
	sendToChan(rf.appendCh)
	//outdate snapshot, discard it
	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		return
	}
	if args.LastIncludedIndex < (rf.lastIncludedIndex + len(rf.logs) - 1) {
		rf.logs = append(make([]LogEntry, 0), rf.logs[args.LastIncludedIndex-rf.lastIncludedIndex:]...)
	} else {
		rf.logs = []LogEntry{{args.LastIncludedTerm, nil}}
	}
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.persistStateMachineAndRaftState(args.Data)
	rf.commitIndex = Max(rf.commitIndex, rf.lastIncludedIndex)
	rf.lastApplied = Max(rf.lastApplied, rf.lastIncludedIndex)
	if rf.lastApplied > rf.lastIncludedIndex {
		return
	}
	applyMsg := ApplyMsg{
		CommandValid: false,
		Snapshot:     args.Data,
	}
	rf.applyCh <- applyMsg
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// 其他的server太落后了，被动发送快照
// 別忘了最后还要更新matchIndex以及nextIndex
func (rf *Raft) sendSnapshot(peer int) {
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.persister.ReadSnapshot(),
	}
	rf.mu.Unlock()
	reply := InstallSnapshotReply{}
	ok := rf.sendInstallSnapshot(peer, &args, &reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != LEADER || rf.currentTerm != args.Term {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.beFollower(reply.Term)
		return
	}
	rf.updateMatchIndex(peer, rf.lastIncludedIndex)
}

func (rf *Raft) DoSnapshot(logIndex int, stateMachineData []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if logIndex <= rf.lastIncludedIndex {
		return
	}
	rf.lastIncludedTerm = rf.logs[logIndex-rf.lastIncludedIndex].Term
	rf.logs = append(make([]LogEntry, 0), rf.logs[logIndex-rf.lastIncludedIndex:]...)
	rf.lastIncludedIndex = logIndex
	rf.persistStateMachineAndRaftState(stateMachineData)
}
