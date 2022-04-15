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
	"fmt"
	"log"
	"math/rand"
	"os"

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
const heartBeatInterval = 100

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	//
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}
type Log struct {
	Term int
	command string
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

	//稳定状态
	CurrentTerm int //当前任期
	VotedFor    int //投票给那个candidate，-1表示未投票
	log         []Log //日志
	//server的不稳定状态
	CommitIndex int 
	LastApplied int
	State       State
	//leader的不稳定状态
	NextIndex  []int
	MatchIndex []int

	electionIntervalTime int

	electionStopChan      chan bool
	isStartElectionChan chan bool
	electionTimeChan      chan string
	voteReplyChan chan *RequestVoteReply
	AppendEntriesChan chan AppendEntriesReply

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
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
// 恢复raft的状态
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
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


func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		rf.mu.Lock()
		log.Printf("raft  %d  state %d startSendAppendEntries to %d\n  failed", rf.me, rf.State, server )
		rf.mu.Unlock()
	}
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
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


func (rf *Raft) startSendAppendEntries() {
		rf.mu.Lock()
		args := &AppendEntriesArgs{}
		args.Term = rf.CurrentTerm
		args.LeaderId = rf.me
		rf.resetElectionIntervalTime()
		rf.mu.Unlock()

		reply := &AppendEntriesReply{}

		for serverId := 0; serverId < len(rf.peers); serverId++ {
			if serverId != rf.me {
				log.Printf("raft  %d  state %d startSendAppendEntries to %d\n ", rf.me,rf.State, serverId )
				ok := rf.sendAppendEntries(serverId, args, reply)
				if !ok {
					continue
				}
				rf.mu.Lock()
				if rf.State != leader {
					//遇到的神奇bug
					//如果不return的话，可能一直处于发送AppendEntries中，无法改变状态
					rf.mu.Unlock()
					return
				}
				if reply.Term > rf.CurrentTerm{
					rf.setNewTerm(reply.Term)
					rf.mu.Unlock()
					break
				}else{
					rf.mu.Unlock()
				}
			}
		}
	log.Printf("raft  %d startSendAppendEntries over\n", rf.me )
}



func (rf *Raft) eletionTimer(){
	log.Printf("raft %d initEletionTimer\n", rf.me)
	sleepTime := rand.Intn(maxRandDis - minRandDis) + minRandDis
	time.Sleep(time.Duration(sleepTime) * time.Millisecond)
	log.Printf("raft %d electionTimeOut\n", rf.me)
	rf.mu.Lock()
	if rf.State == candidate {
		log.Printf("raft %d stop Election\n", rf.me)
		rf.electionStopChan <- true
		//是否需要关闭

	}
	rf.mu.Unlock()
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
	if term > rf.CurrentTerm || rf.CurrentTerm == 0 {
		rf.State = follow
		rf.CurrentTerm = term
		rf.VotedFor = -1
		rf.resetElectionIntervalTime()
		log.Printf("[%d]: set term %v\n", rf.me, rf.CurrentTerm)

		//rf.persist()
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
	rf.log =make([]Log, 1)
	rf.VotedFor = -1

	rf.CommitIndex = 0
	rf.LastApplied = 0
	rf.State = follow


	rf.electionIntervalTime = rand.Intn(maxRandDis - minRandDis) + minRandDis



	rf.electionStopChan = make(chan bool, 1)
	rf.AppendEntriesChan = make(chan  AppendEntriesReply, len(peers))
	rf.voteReplyChan = make(chan *RequestVoteReply, len(rf.peers))//重置通道防止上次选举的消息写入

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections

	file := "./" +"message"+ ".txt"

	logFile, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0766)
	//defer logFile.Close()

	if err != nil {
		panic(err)
	}
	log.SetOutput(logFile)
	log.Printf("raft %d finishInitial\n", rf.me)
	fmt.Println(rf.electionIntervalTime)

	go rf.ticker()
	return rf
}

