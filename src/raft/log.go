package raft

import "log"

type Log struct {
	Index int
	Term int
	Command interface{}
}


//leader 添加日志
func (rf *Raft) addLog(log *Log)  {
	rf.log = append(rf.log, *log)
}



func (rf *Raft) getSendingEntries(id int) []Log{
	//可能的情况
	//0条(心跳)，1条，多条，如何确定
	log.Printf("log length  =  %d  nextIndex = %d entries len %d\n",
		len(rf.log), rf.NextIndex[id], len(rf.log)  - rf.NextIndex[id] )
	entries := make([]Log, len(rf.log)  - rf.NextIndex[id] )
	if len(rf.log) > rf.NextIndex[id] {
		for  i, j := rf.NextIndex[id], 0; i < len(rf.log); i, j = i + 1, j + 1{
			entries[j]  = rf.log[i]
		}
	}
	return entries
}


func (rf *Raft) getLastLog() Log{
	return rf.log[len(rf.log) - 1]
}

func (rf *Raft) getLogAtIndex(index int) Log{
	return rf.log[index]
}
func (rf *Raft) findLastLogInTerm(x int) int {
	for i := rf.getLastLog().Index; i > 0; i-- {
		term := rf.getLogAtIndex(i).Term
		if term == x {
			return i
		} else if term < x {
			break
		}
	}
	return -1
}