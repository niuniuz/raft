package raft

import "log"

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func min(x int, y int)  int{
	if x > y {
		return x
	}else {
		return y
	}
}

func isChanClose(ch chan bool) bool {
	select {
	case _, received := <- ch:
		return !received
	default:
	}
	return false
}

