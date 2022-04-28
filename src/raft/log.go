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
	var  entries []Log
	log.Printf("raft %d log length  =  %d  nextIndex = %d entries len %d\n",
		rf.me, rf.getLastLog().Index, rf.NextIndex[id], rf.getLastLog().Index - rf.NextIndex[id])

	if rf.getLastLog().Index < rf.NextIndex[id] {
		//没有待发送的日志，发送空心跳
		entries = make([]Log, 0)
	}else{
		entries = make([]Log, rf.getLastLog().Index - rf.NextIndex[id] + 1)
	}

	if rf.getLastLog().Index >= rf.NextIndex[id] {
		for  i, j := rf.NextIndex[id], 0; i <=  rf.getLastLog().Index; i, j = i + 1, j + 1{
			entries[j]  = rf.getLogAtIndex(i)
		}
	}
	return entries
}


func (rf *Raft) getLastLog() Log{
	return rf.log[len(rf.log) - 1]
}

func (rf *Raft) getFirstLog() Log{
	return rf.log[0]
}

//删除从index之前的数组值
func (rf *Raft) trimLog(firstindex int, lastindex int) []Log{
	entry := make([]Log, 1)
	entry[0].Term = 0
	entry[0].Index = 0
	entry[0].Command = nil
	if rf.getLastLog().Index < firstindex - 1 {
		return  append(entry)
	}
	log.Printf("raft %d firstindex = %d  lastindex = %d", rf.me, firstindex, lastindex)
	//rf.log = rf.log[firstindex -  rf.lastIncludedTerm : lastindex -  rf.lastIncludedTerm + 1]
	for i := firstindex -  rf.lastIncludedIndex; i < lastindex -  rf.lastIncludedIndex + 1; i++  {
		entry = append(entry, rf.log[i])
	}
	return entry
}

func (rf *Raft) getLogAtIndex(index int) Log{
	return rf.log[index - rf.lastIncludedIndex]
	//log被截断后，index对应的log位置发生变化
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
			rf.log = append(rf.log[:i - rf.lastIncludedIndex])
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