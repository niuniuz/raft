package raft

import (
	"log"
)

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries [] Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
	Conflict bool
	XTerm    int
	XIndex   int
	XLen     int
}

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

type InstallSnapshotArgs struct {
	// Your data here (2A, 2B).
	Term int
	LeaderId int
	LastIncludedIndex int
	LastIncludedTerm int
	Offset int
	Data [] byte
	Done bool
}

type InstallSnapshotArgsReply struct {
	Term int
}


func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	cadidateTerm := args.Term

	if  cadidateTerm > rf.CurrentTerm {
		rf.setNewTerm(cadidateTerm)
	}

	if cadidateTerm < rf.CurrentTerm{
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		return
	}

	isArgsLogNewThanSelfLog := rf.getLastLog().Term < args.LastLogTerm ||
	(rf.getLastLog().Index <= args.LastLogIndex && rf.getLastLog().Term == args.LastLogTerm)

	if (rf.VotedFor == -1  || rf.VotedFor == args.CandidateId) && isArgsLogNewThanSelfLog{
		reply.VoteGranted = true
		rf.VotedFor = args.CandidateId
		rf.persist()
		log.Printf("raf %d VotedFor %d \n",rf.me, rf.VotedFor )
		rf.resetElectionIntervalTime()
	} else  {
		reply.VoteGranted = false
	}

	reply.Term =  rf.CurrentTerm

}

func (rf * Raft) AppendEntries(args * AppendEntriesArgs, reply * AppendEntriesReply){
	//重置选举时间
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Success = false
	reply.Term = rf.CurrentTerm

	log.Printf("raft %d  term %d receive AppendEntries from raft %d term %d\n",
		rf.me, rf.CurrentTerm, args.LeaderId,  args.Term)
	log.Printf("raft %d  term %d lastIncludedIndex %d logs %d lastConcludeIndex %d args.PrevLogIndex %d\n",
		rf.me, rf.CurrentTerm, rf.lastIncludedIndex, rf.log,  rf.lastIncludedIndex, args.PrevLogIndex)

	if args.Term > rf.CurrentTerm  {
		//可能有bug
		rf.setNewTerm(args.Term)
		//return
		// 是否需要更新日志操作
	}

	if args.Term < rf.CurrentTerm{
		log.Println("args.Term < rf.CurrentTerm")
		return
	}
	if rf.State == candidate{
		rf.setNewTerm(args.Term)
	}


	rf.resetElectionIntervalTime()
	if rf.getLastLog().Index < args.PrevLogIndex {
		log.Println("rf.getLogAtIndex(args.PrevLogIndex).Index != args.PrevLogIndex ")
		reply.Conflict = true
		reply.XLen = len(rf.log)
		reply.XIndex = -1
		reply.XTerm = -1
		return
	}


	if args.Term == rf.CurrentTerm  {
		if rf.getLogAtIndex(args.PrevLogIndex).Term != args.PrevLogTerm {
			reply.Conflict = true
			xTerm := rf.getLogAtIndex(args.PrevLogIndex).Term
			for xIndex := args.PrevLogIndex; xIndex > rf.lastIncludedIndex; xIndex-- {
				if rf.getLogAtIndex(xIndex-1).Term != xTerm {
					reply.XIndex = xIndex
					break
				}
			}
			reply.XTerm = xTerm
			reply.XLen = len(rf.log)
			log.Println("rf.getLogAtIndex(args.PrevLogIndex).Term != args.Term")
			log.Printf("raft %d rf.getLogAtIndex(args.PrevLogIndex).Term %d args.Term %d",rf.me, rf.getLogAtIndex(args.PrevLogIndex).Term , args.PrevLogTerm)
			return
		}
		//更新日志
		rf.followLogUpdate(args)
	}

	if args.LeaderCommit > rf.CommitIndex {
		log.Printf("raft %d  lastLogIndex = %d \n", rf.me, rf.getLastLog().Index)
		rf.CommitIndex = min(args.LeaderCommit, rf.getLastLog().Index)
		rf.apply()
		log.Printf("raft %d  term = %d CommitIndex = %d\n", rf.me, rf.CurrentTerm, rf.CommitIndex)
	}


	log.Printf("raft  %d  entries %v commitIndex %d\n", rf.me, rf.log, rf.CommitIndex)
	reply.Success = true

}



func (rf *Raft)InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotArgsReply){
	//InstallSnapshot rule 1
	rf.mu.Lock()
	log.Printf("raft  %d  state %d receiveInstallSnapshots from %d \n ", rf.me,rf.State,  args.LeaderId )
	reply.Term = rf.CurrentTerm
	if args.Term < rf.CurrentTerm{
		rf.mu.Unlock()
		log.Println("args.Term < rf.CurrentTerm")
		return
	}
	if args.Term > rf.CurrentTerm {
		rf.setNewTerm(args.Term)
		rf.persist()
	}else{
		rf.resetElectionIntervalTime()
	}

	//if args.LastIncludedIndex <= rf.CommitIndex {
	//	rf.mu.Unlock()
	//	log.Println("args.LastIncludedIndex <= rf.CommitIndex")
	//	return
	//}
	//w := new(bytes.Buffer)
	//e := labgob.NewEncoder(w)
	//e.Encode(rf.CurrentTerm)
	//e.Encode(rf.VotedFor)
	//e.Encode(rf.log)
	//data := w.Bytes()
	//rf.persister.SaveStateAndSnapshot(data, args.Data)

	log.Printf("raft  %d  state %d save InstallSnapshots from %d \n ", rf.me,rf.State,  args.LeaderId )
	//InstallSnapshot rule 2,for lab3
	//if args.Offset == 0 {
	//
	//}
	rf.mu.Unlock()
	go func() {
		rf.mu.Lock()
		applyMsg := &ApplyMsg{
			SnapshotValid: true,
			Snapshot: args.Data,
			SnapshotTerm: args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
		rf.mu.Unlock()
		rf.applyCh <- *applyMsg
	}()

}