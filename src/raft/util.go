package raft

import "log"

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func Min(x, y int) int {
	if x > y {
		return y
	}
	return x
}

func Max(x, y int) int {
	if x < y {
		return y
	}
	return x
}

func sendToChan(ch chan bool) {
	select {
	case <-ch:
	default:
	} // 旧的消息还没被拿出来，但是新的消息已经来了，直接以新的消息为主去刷新vote过期
	ch <- true
}
