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
		rf.VotedFor = args.CandidateId
		rf.persist()
		log.Printf("raf %d VotedFor %d \n",rf.me, rf.VotedFor )
		reply.VoteGranted = true
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

	if args.Term > rf.CurrentTerm  {
		//可能有bug
		rf.setNewTerm(args.Term)
		return
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
		log.Println("rf.getLogAtIndex(args.PrevLogIndex).Index != args.PrevLogIndex")
		reply.Conflict = true
		reply.XLen = len(rf.log)
		reply.XIndex = -1
		reply.XTerm = -1
		return
	}

	if args.Term == rf.CurrentTerm {
		if rf.getLogAtIndex(args.PrevLogIndex).Term != args.PrevLogTerm {
			reply.Conflict = true
			xTerm := rf.getLogAtIndex(args.PrevLogIndex).Term
			for xIndex := args.PrevLogIndex; xIndex > 0; xIndex-- {
				if rf.getLogAtIndex(xIndex-1).Term != xTerm {
					reply.XIndex = xIndex
					break
				}
			}
			reply.XTerm = xTerm
			reply.XLen = len(rf.log)
			log.Println("rf.getLogAtIndex(args.PrevLogIndex).Term != args.Term")
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

func (rf *Raft)followLogUpdate(args * AppendEntriesArgs)  {
	log.Printf("raft %d update",  rf.me )
	//未携带entries，判定为心跳
	if len(args.Entries) == 0 {
		return
	}
	//删除follow中未提交的日志, 不应该像下面这样截断，可能收到过期的appendEntires导致已提交的条目被删除
	//if len(rf.log) > args.PrevLogIndex {
	//	rf.log = append(rf.log[:args.PrevLogIndex - 1])
	//}

	//判断是否产生冲突,产生冲突则删除follow中从冲突位置开始的日志
	for i,j := args.PrevLogIndex + 1, 0;  j < len(args.Entries); i, j = i + 1, j + 1{
		if i <= rf.getLastLog().Index && rf.getLogAtIndex(i).Term != args.Entries[j].Term {
			rf.log = append(rf.log[:i])
			rf.log = append(rf.log, args.Entries[j:]...)
			rf.persist()
			break
		}else if i > rf.getLastLog().Index {
			//log.Printf("raft  %d  args.Entries %v \n", rf.me, args.Entries[j:])
			rf.log = append(rf.log, args.Entries[j:]...)
			rf.persist()
			break
		}
	}
}
