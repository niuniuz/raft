package raft

import (
	"log"
)

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) startSendAppendEntries() {
	if rf.killed() == false {
		for serverId := 0; serverId < len(rf.peers); serverId++ {
			if serverId != rf.me {
				//自己的matchIndex
				rf.mu.Lock()
				if rf.lastIncludedIndex >=  rf.NextIndex[serverId] {
					installSnapshotArgs := &InstallSnapshotArgs{
						Term: rf.CurrentTerm,
						LeaderId: rf.me,
						LastIncludedIndex: rf.lastIncludedIndex,
						LastIncludedTerm: rf.lastIncludedTerm,
						Data: rf.snapshot,
					}
					installSnapshotReply := &InstallSnapshotArgsReply{}
					rf.mu.Unlock()
					go rf.sendInstallSnapshot(serverId, installSnapshotArgs, installSnapshotReply)
					continue
				}

				args := &AppendEntriesArgs{
					Term:         rf.CurrentTerm,
					LeaderId:     rf.me,
					Entries:      rf.getSendingEntries(serverId),
					LeaderCommit: rf.CommitIndex,
					PrevLogIndex: rf.NextIndex[serverId] - 1,
					PrevLogTerm:  rf.getLogAtIndex(rf.NextIndex[serverId]-1).Term,
				}


				//log.Printf(" leader raft  %d log %v  commitIndex %d\n", rf.me, rf.log, rf.CommitIndex)
				log.Printf("raft  %dprepare to send entries %v \n", rf.me, args.Entries)

				//可以一次发送多个Entries
				rf.resetElectionIntervalTime()
				if rf.State != leader {
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()
				go rf.leaderSendAppendEntries(args, serverId)
			}
		}
		log.Printf("raft  %d startSendAppendEntries over\n", rf.me )
	}
}

func (rf * Raft) leaderSendAppendEntries(args *AppendEntriesArgs, serverId int)  {
		reply := &AppendEntriesReply{}
		rf.mu.Lock()
		log.Printf("raft  %d  state %d startSendAppendEntries to %d\n ", rf.me,rf.State, serverId )
		rf.mu.Unlock()

		ok := rf.sendAppendEntries(serverId, args, reply)
		if !ok {
			rf.mu.Lock()
			log.Printf("raft  %d  state %d startSendAppendEntries to %d failed\n ", rf.me,rf.State, serverId )
			rf.mu.Unlock()
			return
		}
		log.Printf("raft  %d  get %d replyCondition  %v\n ", rf.me, serverId, reply )

		rf.mu.Lock()
		if rf.State != leader {
			//遇到的神奇bug
			//如果不return的话，可能一直处于发送AppendEntries中，无法改变状态
			rf.mu.Unlock()
			return
		}else{
			rf.mu.Unlock()
		}

		//日志更新失败, 等待下一次发送
		rf.mu.Lock()
		if reply.Term > rf.CurrentTerm{
			rf.setNewTerm(reply.Term)
			rf.mu.Unlock()
			return
		}else{
			rf.mu.Unlock()
		}

		rf.mu.Lock()
		if args.Term ==  rf.CurrentTerm  {
			if reply.Success {
					//不是心跳
					match :=  args.PrevLogIndex + len(args.Entries)
					next := match + 1
					//防止接收过期的rpc
					rf.NextIndex[serverId] = max(rf.NextIndex[serverId], next)
					rf.MatchIndex[serverId] = max(rf.MatchIndex[serverId], match)
					log.Printf("raft  %d matchIndex = %d ",rf.me, match )
			}else if reply.Conflict{
				if reply.Conflict {
					if reply.XTerm == -1 {
						rf.NextIndex[serverId] = reply.XLen
					} else {
						lastLogInXTerm := rf.findLastLogInTerm(reply.XTerm)
						if lastLogInXTerm > 0 {
							rf.NextIndex[serverId] = lastLogInXTerm
						} else {
							rf.NextIndex[serverId] = reply.XIndex
						}
					}
					if rf.NextIndex[serverId] > 1 {
						rf.NextIndex[serverId]--
					}
				}
			}else if rf.NextIndex[serverId] > 1 {
				rf.NextIndex[serverId]--
			}
			rf.commit()
		}
		rf.mu.Unlock()
}
func (rf * Raft)commit()  {
	if rf.State !=  leader {
		return
	}
	for n := rf.CommitIndex + 1; n <= rf.getLastLog().Index; n++{
		if rf.getLogAtIndex(n).Term != rf.CurrentTerm {
			continue
		}
		counter := 1
		for severID,_ := range rf.peers {
			if severID != rf.me && rf.MatchIndex[severID] >= n{
				counter++
			}
			if counter > len(rf.peers) / 2 {
				log.Printf("raft  %d commitIndex = %d ",rf.me, n )
				rf.CommitIndex = n
				rf.apply()
				break
			}
		}
	}

}
