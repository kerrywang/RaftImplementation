package raft

import (
	"log"
)
import "sort"

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func ASSERT_EQUAL(left int, right int) {
	if (left != right) {
		log.Fatalf("%d and %d should be equal", left, right)
	}
}

func MIN(left int, right int) (int)  {
	if (left < right)  {
		return left
	} else{
		return right
	}

}

func MAX(left int, right int) (int)  {
	if (left > right)  {
		return left
	} else{
		return right
	}

}

func (rf *Raft) getCommitIndexLeader() int {
	n_peers := len(rf.peers)
	target_index := (n_peers - 1) / 2
	match_index_cpy := make([]int, n_peers)
	copy(match_index_cpy, rf.matchIndex)
	DPrintf("From Leader: %d Matched Index: %v\n", rf.me, rf.matchIndex)

	sort.Ints(match_index_cpy)

	return match_index_cpy[target_index]
}

func (rf *Raft) updateCommitIndex() {
	cur_commit_index := rf.getCommitIndexLeader()
	if rf.logs[cur_commit_index].Term != rf.currentTerm {
		return
	}
	for cur_commit_index > rf.commitIndex {
		DPrintf("Changing Leader Commit Index from: %d to %d\n", rf.commitIndex, cur_commit_index)
		rf.commitIndex ++
		//if (rf.logs[rf.commitIndex].Term == rf.currentTerm) {
		rf.apply_chan <- ApplyMsg {
			CommandValid: true,
			Command:      rf.logs[rf.commitIndex].Command,
			CommandIndex: rf.logs[rf.commitIndex].Pos,
		}
		//}
		DPrintf("Server: %v  [LEADER] Commit_ID: %d\n", rf.me, rf.commitIndex)

	}
}
