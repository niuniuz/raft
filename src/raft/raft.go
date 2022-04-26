package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/labgob"
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"

	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)


//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//ApplyMsg会被发送到applyCh通道中，上层服务接收到后，将其应用到状态机。
type State int
const (
	follow = iota
	candidate
	leader
)

const baseElectionTimeout = 80 // ms, this this as network latency
// random Disturbance for election timeout.
const minRandDis = 300
const maxRandDis = 600
const heartBeatInterval = 150

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill() kill

	//需要持久化的状态
	CurrentTerm int //当前任期
	VotedFor    int //投票给那个candidate，-1表示未投票
	log         []Log //日志
	//server的不稳定状态
	CommitIndex int 
	LastApplied int
	State       State
	//leader的不稳定状态
	//用于日志同步
	NextIndex  []int
	MatchIndex []int
	electionIntervalTime int

	applyCh   chan ApplyMsg
	applyCond *sync.Cond


	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}

// return currentTerm and whether this server
// believes it is the leader.
// 查询某个server的任期和是否认为自己是leader
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	log.Println("GetState")
	rf.mu.Lock()
	log.Println("isdeadlock")
	defer rf.mu.Unlock()
	term = rf.CurrentTerm
	if rf.State == 2 {
		isleader = true
	}else {
		isleader = false
	}
	rf.persist()
	log.Printf("raft %d isLeader= %v term = %d\n",rf.me, isleader, term)
	// Your code here (2A).

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// 将raft的状态持久化储存
func (rf *Raft) persist() {
	//使用前加锁
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
// 恢复raft的状态
func (rf *Raft) readPersist(data []byte) {
	//使用前加锁
	DPrintf("[%v]: readPersist", rf.me)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []Log

	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil {
		log.Fatal("failed to read persist\n")
	} else {
		rf.CurrentTerm = currentTerm
		rf.VotedFor = votedFor
		rf.log = logs
	}
}


//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}


//
// exCample RequestVote RP arguments structure.
// field names must start with capital letters!
// RPC参数结构体需要大写


//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//


//
// example RequestVote RPC handler.
//



//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//



//
//the service using Raft (e.g. a k/v server) wants to start
//agreement on the next command to be appended to Raft's log. if this
//server isn't the leader, returns false. otherwise start the
//agreement and return immediately. there is no guarantee that this
//command will ever be committed to the Raft log, since the leader
//may fail or lose an election. even if the Raft instance has been killed,
//this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//

//收到日志
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if  rf.State != leader {
		return -1, rf.CurrentTerm, false
	}
	logs := &Log{
		Command:command,
		Index: len(rf.log),
		Term: rf.CurrentTerm,
	}
	rf.addLog(logs)
	rf.persist()
	log.Printf("leader rf %d append %v getLastLog().Index %v", rf.me, logs, rf.getLastLog().Index + 1 )
	return rf.getLastLog().Index , rf.CurrentTerm, true
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.

//定时器
func (rf *Raft) ticker() {
	log.Printf("raft %d initialTicker\n", rf.me)
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		time.Sleep(time.Duration(heartBeatInterval) * time.Millisecond)

		rf.mu.Lock()
		if rf.State == leader {
			rf.mu.Unlock()
			rf.startSendAppendEntries()
		}else {
			rf.mu.Unlock()
		}

		rf.mu.Lock()
		rf.electionIntervalTime -= heartBeatInterval
		if rf.electionIntervalTime <= 0 {
			rf.mu.Unlock()
			rf.startElection()
			log.Printf("raft %d electionEnd\n", rf.me)
		}else  {
			rf.mu.Unlock()
		}
	}
}


func (rf *Raft) resetElectionIntervalTime(){
	//使用前需要加锁
	rf.electionIntervalTime = rand.Intn(maxRandDis - minRandDis) + minRandDis
	log.Printf("raft  %d resetTime\n time =  %d",rf.me,rf.electionIntervalTime)

}


//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//初始化currentTerm,log[], commitIndex,lastApplied

func (rf *Raft) setNewTerm(term int) {
	if term >= rf.CurrentTerm || rf.CurrentTerm == 0 {
		rf.State = follow
		rf.CurrentTerm = term
		rf.VotedFor = -1
		rf.resetElectionIntervalTime()
		log.Printf("[%d]: set term %v\n", rf.me, rf.CurrentTerm)
		rf.persist()
	}
}

func setLog()  {
	file := "./" +"message"+ ".txt"

	logFile, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0766)
	//defer logFile.Close()

	if err != nil {
		panic(err)
	}
	log.SetOutput(logFile)

}


func (rf *Raft) apply() {
	rf.applyCond.Broadcast()
	DPrintf("[%v]: rf.applyCond.Broadcast()", rf.me)
}

func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for !rf.killed() {
		// all server rule C1
		if rf.CommitIndex > rf.LastApplied &&
			rf.getLastLog().Index > rf.LastApplied{
			rf.LastApplied++
			applyMsg := ApplyMsg{
				CommandValid:  true,
				Command:       rf.log[rf.LastApplied].Command,
				CommandIndex:  rf.LastApplied,
			}
			rf.mu.Unlock()
			log.Printf("[%v]: apply Msg %v\n COMMIT %v ", rf.me, applyMsg, rf.commits())
			rf.applyCh <- applyMsg
			rf.mu.Lock()
		} else {
			rf.applyCond.Wait()
			log.Printf("[%v]: rf.applyCond.Wait()", rf.me)
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
	//rf.CurrentTerm = 0
	rf.log = make([]Log, 0)
	rf.log = append(rf.log, Log{0, 0, -1})
	rf.VotedFor = -1

	rf.CommitIndex = 0
	rf.LastApplied = 0

	rf.State = follow
	rf.electionIntervalTime = rand.Intn(maxRandDis - minRandDis) + minRandDis

	rf.NextIndex = make([]int, len(rf.peers))
	rf.MatchIndex = make([]int, len(rf.peers))

	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	setLog()
	// start ticker goroutine to start elections

	log.Printf("raft %d finishInitial\n", rf.me)
	go rf.ticker()
	go rf.applier()
	return rf
}


func (rf *Raft) commits() string {
	nums := []string{}
	for i := 0; i <= rf.LastApplied; i++ {
		nums = append(nums, fmt.Sprintf("%4d", rf.getLogAtIndex(i).Command))
	}
	return fmt.Sprint(strings.Join(nums, "|"))
}