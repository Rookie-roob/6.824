package kvraft

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/Rookie-roob/6.824/src/labgob"
	"github.com/Rookie-roob/6.824/src/labrpc"
	"github.com/Rookie-roob/6.824/src/raft"
)

const Debug = 1

const RequstTimeout = 600

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	OpType    string
	Key       string
	Value     string
	ClientID  int64
	CommandID int
}

type KVServer struct {
	mu        sync.Mutex
	me        int
	rf        *raft.Raft
	applyCh   chan raft.ApplyMsg
	persister *raft.Persister

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	CidDic     map[int64]int
	notifyChan map[int]chan Op //log index为key
	kvdata     map[string]string
	closeChan  chan struct{}
}

func (kv *KVServer) getNotifyChan(logIndex int) chan Op {
	_, ok := kv.notifyChan[logIndex]
	if !ok {
		kv.notifyChan[logIndex] = make(chan Op, 1)
	} // 万一这一步之后，返回之前，就直接被delete掉这个map条目，因此外面需要加锁
	return kv.notifyChan[logIndex]
}

func getMessage(ch chan Op) Op {
	select {
	case reply := <-ch:
		return reply
	case <-time.After(time.Duration(RequstTimeout) * time.Millisecond):
		return Op{}
	}
}

func (kv *KVServer) ifOpSame(originOp Op, newOp Op) bool {
	return originOp.Key == newOp.Key && originOp.Value == newOp.Value &&
		originOp.OpType == newOp.OpType && originOp.ClientID == newOp.ClientID &&
		originOp.CommandID == newOp.CommandID
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		OpType:    "Get",
		Key:       args.Key,
		Value:     "",
		ClientID:  args.ClientID,
		CommandID: args.CommandID,
	} // 注意这里一定要拷贝一波，不能直接传args的引用到Start中
	index, _, isLeader := kv.rf.Start(op)
	reply.WrongLeader = true
	if !isLeader {
		return
	}
	kv.mu.Lock()
	ch := kv.getNotifyChan(index) // 相当于让get chan的这一步原子
	kv.mu.Unlock()                //select前一定要先解锁！
	replyOp := getMessage(ch)
	if kv.ifOpSame(op, replyOp) { //这里要和原来的op进行比较，可能传上来的不是同一个！
		reply.WrongLeader = false
		kv.mu.Lock()
		reply.Value = kv.kvdata[op.Key]
		kv.mu.Unlock()
	}
	// 这里异步是完全没问题的，因为apply的logindex只会往前涨
	go func() {
		kv.mu.Lock()
		delete(kv.notifyChan, index)
		kv.mu.Unlock()
	}()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		OpType:    args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClientID:  args.ClientID,
		CommandID: args.CommandID,
	} // 注意这里一定要拷贝一波，不能直接传args的引用到Start中
	index, _, isLeader := kv.rf.Start(op)
	reply.WrongLeader = true
	if !isLeader {
		return
	}
	kv.mu.Lock()
	ch := kv.getNotifyChan(index)
	kv.mu.Unlock()
	replyOp := getMessage(ch)
	if kv.ifOpSame(op, replyOp) {
		reply.WrongLeader = false
	}
	// 这里异步是完全没问题的，因为apply的logindex只会往前涨
	go func() {
		kv.mu.Lock()
		delete(kv.notifyChan, index)
		kv.mu.Unlock()
	}()
}

func sendToChan(notifyChan chan Op, op Op) {
	select {
	case <-notifyChan:
	default:
	}
	notifyChan <- op
}

func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	close(kv.closeChan)
}

func (kv *KVServer) processApplyFromRaft() {
	for {
		select {
		case <-kv.closeChan:
			return
		case applyData := <-kv.applyCh:
			if applyData.CommandValid {
				op := applyData.Command.(Op)
				kv.mu.Lock()
				maxCommandID, ok := kv.CidDic[op.ClientID]
				if !ok || maxCommandID < op.CommandID {
					switch op.OpType {
					case "Put":
						kv.kvdata[op.Key] = op.Value
					case "Append":
						kv.kvdata[op.Key] += op.Value
					}
					if op.OpType != "Get" {
						kv.CidDic[op.ClientID] = op.CommandID
					}
				}
				ch := kv.getNotifyChan(applyData.CommandIndex)
				kv.mu.Unlock()
				ifSnapshot := kv.checkSnapshot()
				if ifSnapshot {
					go kv.rf.DoSnapshot(applyData.CommandIndex, kv.encodeSnapshot()) //注意这个操作每个server都是有可能主动snapshot的！！！
				}
				sendToChan(ch, op)
			} else {
				kv.decodeKVSnapshot(applyData.Snapshot)
			}
		}
	}
}

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.persister = persister

	// You may need initialization code here.
	kv.CidDic = make(map[int64]int)
	kv.notifyChan = make(map[int]chan Op)
	kv.kvdata = make(map[string]string)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.closeChan = make(chan struct{})
	// You may need initialization code here.
	kv.decodeKVSnapshot(persister.ReadSnapshot()) // 刚boot或者reboot的时候也需要读取快照，如果之前有快照数据的话就要进行读入
	go kv.processApplyFromRaft()

	return kv
}

// snapshot相关
func (kv *KVServer) checkSnapshot() bool {
	if kv.maxraftstate == -1 {
		return false
	}
	return kv.persister.RaftStateSize() >= kv.maxraftstate // 这一步去获取raft层大小时，不要去获取rf的lock，不然容易死锁！！！！
}

func (kv *KVServer) encodeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.CidDic)
	e.Encode(kv.kvdata)
	data := w.Bytes()
	return data
}

func (kv *KVServer) decodeKVSnapshot(snapshotKVData []byte) {
	if snapshotKVData == nil || len(snapshotKVData) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(snapshotKVData)
	d := labgob.NewDecoder(r)
	var commandMap map[int64]int
	var kvdata map[string]string
	if d.Decode(&commandMap) != nil ||
		d.Decode(&kvdata) != nil {
		fmt.Println("decode error!!!")
	} else {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		kv.CidDic = commandMap
		kv.kvdata = kvdata
	}
}
