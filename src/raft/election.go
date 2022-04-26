package raft

import (

	"log"
	"sync"
)

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs,
	voteCount * int, becomeLeader * sync.Once ) bool {
	reply := &RequestVoteReply{}
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		return ok
	}
	if reply.Term != args.Term {
		return ok
	}

	if !reply.VoteGranted {
		return ok
	}


	if reply.VoteGranted == true {
		rf.mu.Lock()
		*voteCount++
		log.Printf("raft  %d voteCount = %d\n", rf.me, *voteCount)
		if *voteCount >= len(rf.peers)/2+1 &&
			rf.State == candidate &&
			args.Term == rf.CurrentTerm {
			//注意这个地方的判断条件
			becomeLeader.Do(func() {
				log.Printf("raft  %d electionSuccess \n", rf.me)
				//选举为leader后状态的转变
				rf.State = leader
				rf.persist()
				for i := 0; i < len(rf.peers); i++ {
					rf.NextIndex[i] = len(rf.log)
					rf.MatchIndex[i] = 0
				}

				log.Printf("raft  %d state change to  %d\n", rf.me, rf.State)
				log.Printf("raft  %d election prepareToEnd\n", rf.me)
				//close(rf.electionStopChan)
				rf.mu.Unlock()
				rf.startSendAppendEntries()
				log.Printf("raft  %d election prepareToEnd\n", rf.me)

			})
		} else {
			rf.mu.Unlock()
		}
		return ok
	}
	return ok
}

func (rf *Raft) startElection() {
	if rf.killed() == false {
		var becomeLeader  sync.Once
		rf.mu.Lock()
		log.Printf("raft  %d startElection\n", rf.me )
		rf.State = candidate
		rf.VotedFor = rf.me
		rf.CurrentTerm += 1
		rf.persist()
		//rf.VotedFor = rf.me
		rf.resetElectionIntervalTime()
		requestVoteArgs := &RequestVoteArgs{
			Term: rf.CurrentTerm,
			CandidateId: rf.me,
			LastLogIndex: rf.getLastLog().Index,
			LastLogTerm: rf.getLastLog().Term,
		}
		rf.mu.Unlock()

		voteCount := 1
		for  i := 0; i < len(rf.peers) ;i++{
			if i != rf.me && rf.killed() == false{
				log.Printf("raft  %d sendRequestVote to %d\n",rf.me, i )
				go rf.sendRequestVote(i, requestVoteArgs, &voteCount, &becomeLeader)
			}
		}
	}
}