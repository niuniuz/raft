package raft

import (
	"6.824/labgob"
	"bytes"
	"log"
)

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotArgsReply)  {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	if reply.Term > rf.CurrentTerm {
		rf.setNewTerm(reply.Term)
		rf.mu.Unlock()
		return
	}
	rf.MatchIndex[server] = args.LastIncludedIndex
	rf.NextIndex[server] = rf.MatchIndex[server] + 1
	rf.mu.Unlock()

}



func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2D).
	log.Printf("raft %d CondInstallSnapshot" ,rf.me)
	//if lastIncludedIndex <= rf.CommitIndex {
	//	return  false
	//}

	if rf.lastIncludedTerm > lastIncludedTerm || rf.lastIncludedIndex > lastIncludedIndex {
		return false
	}

	if lastIncludedIndex > rf.getLastLog().Index{
		// 快照lastIncludedIndex大于log中最新条目的日志，删除所有日志
		entry := make([]Log, 1)
		rf.log = entry

	}else {
		rf.log = rf.trimLog(lastIncludedIndex + 1, rf.getLastLog().Index)
	}

	rf.log[0].Command = nil
	rf.log[0].Index = rf.lastIncludedIndex
	rf.log[0].Term = rf.lastIncludedTerm

	rf.snapshot = snapshot
	rf.lastIncludedTerm = lastIncludedTerm
	rf.lastIncludedIndex = lastIncludedIndex
	rf.LastApplied = lastIncludedIndex
	rf.CommitIndex = lastIncludedIndex

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)

	rf.persister.SaveStateAndSnapshot(w.Bytes(), snapshot)
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	//rf.log = copyLogHigher(index, rf.log)
	//rf.persist()
	//if rf.State != Leaders {
	//	return
	//}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.lastIncludedIndex >= index {
		return
	}


	if rf.LastApplied < index {
		log.Printf("raft %d rf.LastApplied" ,rf.me)
		return
	}
	log.Printf("raft %d Snapshot index = %d" ,rf.me, index)
	rf.snapshot = snapshot

	rf.lastIncludedTerm = rf.getLogAtIndex(index).Term
	rf.log = rf.trimLog(index + 1, rf.getLastLog().Index)
	rf.lastIncludedIndex = index

	log.Printf("raft %d lastIncludedIndex %d lastIncludedTerm = %d" ,
		rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm)

	rf.log[0].Index, rf.log[0].Term = rf.lastIncludedIndex, rf.lastIncludedTerm
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	rf.persister.SaveStateAndSnapshot(w.Bytes(), snapshot)
	log.Printf("raft %d log after snapshot = %d" ,rf.me, rf.log)


}