package raft

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/Rookie-roob/6.824/src/labgob"
	"github.com/Rookie-roob/6.824/src/labrpc"
)

// import "bytes"
// import "../labgob"

const (
	FOLLOWER             int           = 0
	CANDIDATE            int           = 1
	LEADER               int           = 2
	MIN_ELECTION_TIMEOUT int           = 200
	MAX_ELECTION_TIMEOUT int           = 500
	HEARTBEAT_INTERVAL   time.Duration = 100 * time.Millisecond
)

type ApplyMsg struct {
	CommandValid bool // 为false的话表示为快照消息
	Command      interface{}
	CommandIndex int

	//Snapshot relevant
	Snapshot []byte
}

type LogEntry struct {
	Term       int
	LogDetails interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//Persistent State
	currentTerm int
	votedFor    int
	logs        []LogEntry

	//Volatile State
	commitIndex int
	lastApplied int
	state       int // 0 : follower; 1 : candidate; 2 : leader
	nextIndex   []int
	matchIndex  []int // Leader中，appendEntries以及sendSnapshot成功会更新matchIndex
	applyCh     chan ApplyMsg

	appendCh chan bool
	voteCh   chan bool
	killCh   chan bool
	//Snapshot state
	lastIncludedIndex int
	lastIncludedTerm  int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	rf.mu.Lock()
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.state == LEADER
	rf.mu.Unlock()
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	/*
		// Your code here (2C).
		// Example:
		// w := new(bytes.Buffer)
		// e := labgob.NewEncoder(w)
		// e.Encode(rf.xxx)
		// e.Encode(rf.yyy)
		// data := w.Bytes()
		// rf.persister.SaveRaftState(data)
	*/
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var logs []LogEntry
	var votedFor int
	var lastIncludedIndex int
	var lastIncludedTerm int // 在有snapshot的情况下，lastIncludedIndex以及lastIncludedTerm也要持久化，不然reboot后根本就不知道日志index开始点
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil { // 传切片的引用和传切片意义不同
		fmt.Println("decode error!!!")
	} else {
		rf.mu.Lock()
		rf.currentTerm = currentTerm
		rf.logs = logs
		rf.votedFor = votedFor
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.commitIndex = rf.lastIncludedIndex
		rf.lastApplied = rf.lastIncludedIndex
		rf.mu.Unlock()
	}
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isLeader = rf.state == LEADER
	if rf.state != LEADER {
		return index, term, false
	}
	index = len(rf.logs) + rf.lastIncludedIndex
	logEntry := LogEntry{Term: rf.currentTerm, LogDetails: command}
	rf.logs = append(rf.logs, logEntry)
	term = rf.currentTerm
	rf.persist()
	rf.broadcastHeartbeat()
	return index, term, isLeader
}

func (rf *Raft) Kill() {
	sendToChan(rf.killCh)
}

/*
**go中类的方法和成员的可见性都是根据其首字母的大小写来决定的，
**如果变量名、属性名、函数名或方法名首字母大写，就可以在包外直接访问这些变量、属性、函数和方法，否则只能在包内访问，
**因此 Go 语言类属性和成员方法的可见性都是包一级的，而不是类一级的。
 */
func (rf *Raft) GetRaftStateSize() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

func (rf *Raft) service() {
	for {
		select {
		case <-rf.killCh:
			return
		default:
		}
		rf.mu.Lock()
		currentState := rf.state
		rf.mu.Unlock()
		electionTimeout := time.Duration(MIN_ELECTION_TIMEOUT+rand.Intn(MAX_ELECTION_TIMEOUT-MIN_ELECTION_TIMEOUT)) * time.Millisecond
		switch currentState {
		case FOLLOWER, CANDIDATE:
			select {
			case <-rf.voteCh:
			case <-rf.appendCh:
			case <-time.After(electionTimeout):
				rf.mu.Lock()
				rf.beCandidate()
				rf.mu.Unlock()
			}
		case LEADER:
			time.Sleep(HEARTBEAT_INTERVAL)
			rf.broadcastHeartbeat()
		}
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = make([]LogEntry, 1)

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.state = FOLLOWER
	rf.nextIndex = []int{}
	rf.matchIndex = []int{}
	rf.applyCh = applyCh

	rf.voteCh = make(chan bool, 1)
	rf.appendCh = make(chan bool, 1)
	rf.killCh = make(chan bool, 1)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.service()
	return rf
}

func (rf *Raft) updateMatchIndex(server int, matchIndex int) {
	rf.matchIndex[server] = matchIndex
	rf.nextIndex[server] = matchIndex + 1
	rf.updateCommitIndex()
}

// commit logs
func (rf *Raft) Commit() {
	rf.lastApplied = Max(rf.lastApplied, rf.lastIncludedIndex)
	rf.commitIndex = Max(rf.commitIndex, rf.lastIncludedIndex)
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		command := rf.logs[rf.lastApplied-rf.lastIncludedIndex].LogDetails
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      command,
			CommandIndex: rf.lastApplied,
		}
		rf.applyCh <- applyMsg
	}
}

func (rf *Raft) updateCommitIndex() {
	rf.matchIndex[rf.me] = rf.lastIncludedIndex + len(rf.logs) - 1
	// if there exists an N such that N > commitIndex, a majority of
	// matchIndex[i] >= N, and log[N].term == currentTerm, set commitIndex = N
	for n := rf.matchIndex[rf.me]; n >= rf.commitIndex; n-- {
		count := 1
		if rf.logs[n-rf.lastIncludedIndex].Term == rf.currentTerm {
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me && rf.matchIndex[i] >= n {
					count++
				}
			}
		} else if rf.logs[n-rf.lastIncludedIndex].Term < rf.currentTerm {
			break
		}
		if count > len(rf.peers)/2 {
			rf.commitIndex = n
			rf.Commit()
			break
		}
	}
}

func (rf *Raft) getLastLogTerm() int {
	lastIdx := rf.lastIncludedIndex + len(rf.logs) - 1
	if lastIdx < rf.lastIncludedIndex {
		return -1
	}
	return rf.logs[lastIdx-rf.lastIncludedIndex].Term
}

func (rf *Raft) beLeader() {
	if rf.state != CANDIDATE {
		return
	}
	rf.state = LEADER
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	initialIndex := rf.lastIncludedIndex + len(rf.logs)
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = initialIndex
	}
}

func (rf *Raft) beCandidate() {
	rf.state = CANDIDATE
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
	go rf.broadcastVoteReq()
}

func (rf *Raft) beFollower(currentTerm int) {
	rf.state = FOLLOWER
	rf.currentTerm = currentTerm
	rf.votedFor = -1
	rf.persist()
}
