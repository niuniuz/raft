package raft

import (
	"log"
)

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTermEntries []int
	LeaderCommit int
}
type AppendEntriesReply struct {
	Term int
	Success bool
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
	if cadidateTerm < rf.CurrentTerm{
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		return
	}

	if  cadidateTerm > rf.CurrentTerm {
		rf.setNewTerm(cadidateTerm)
	}



	if rf.VotedFor == -1  || rf.VotedFor == args.CandidateId  {
		rf.VotedFor = args.CandidateId
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
	log.Printf("raft %d receive AppendEntries from raft %d", rf.me, args.LeaderId)
	if args.Term > rf.CurrentTerm {
		reply.Term = rf.CurrentTerm//可能有bug
		rf.setNewTerm(args.Term)
	}else if args.Term == rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		switch rf.State {
		case follow:
			rf.resetElectionIntervalTime()
		case candidate:
			rf.resetElectionIntervalTime()
		case leader:

		}
	}else if args.Term < rf.CurrentTerm{
		reply.Term = rf.CurrentTerm
	}
	log.Printf("raft %d  term = %d",rf.me, rf.CurrentTerm )

}