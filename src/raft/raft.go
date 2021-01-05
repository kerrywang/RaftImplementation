package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"log"
	"math/rand"
	"sort"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

import "../labgob"

type StateType string
const (
	Candidate StateType = "Candidate"
	Follower = "Follower"
	Leader = "Leader"
)

const TimeOutStartRange = 600
const TimeOutEndRange = 1500
const HeartBeatInterval = 150
//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//(Updated on stable storage before responding to RPCs)
type PersistenState struct {
	// latest term server has seen (initialized to 0 on first boot, increases monotonically)
	currentTerm int

	// candidateId that received vote in current term (or -1 if none)
	votedFor int

	// log entries; each entry contains command for state machine, and term when entry  was received by leader (first index is 1)
	logs   	[]Log
}


type VolatileStateAllServer struct {
	// index of highest log entry known to be
	// committed (initialized to 0, increases
	// monotonically)
	commitIndex int

	//index of highest log entry applied to state
	//machine (initialized to 0, increases
	//monotonically)
	lastApplied int
}

// (Reinitialized after election)
type VolatileStateLeader struct {
	// for each server, index of the next log entry
	// to send to that server (initialized to leader
	// last log index + 1)
	nextIndex []int

	// for each server, index of highest log entry
	// known to be replicated on server
	// (initialized to 0, increases monotonically)
	matchIndex []int
}
//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	leader_Cond *sync.Cond 		  // Condition that will be signaled when server becomes leader
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	apply_chan chan ApplyMsg
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	persitent_state PersistenState

	volatile_all_server VolatileStateAllServer

	volatile_leader VolatileStateLeader

	cur_state StateType

	last_synced_time time.Time

	time_out_period int
}

type Log struct {
	Command interface{}
	Term int
	Pos int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int = rf.persitent_state.currentTerm
	var isleader bool = rf.cur_state == Leader
	// Your code here (2A).
	return term, isleader
}

// must be called under mutex protection
func (rf *Raft) maybeUpdateTerm(term int) {
	if (term > rf.persitent_state.currentTerm) {
		rf.persitent_state.currentTerm = term
		rf.cur_state = Follower
		rf.persitent_state.votedFor = -1
		rf.persist()
	}
}

func (rf *Raft) getLastIndex() int {
	ASSERT_EQUAL(len(rf.persitent_state.logs) - 1, rf.persitent_state.logs[len(rf.persitent_state.logs) - 1].Pos)
	return rf.persitent_state.logs[len(rf.persitent_state.logs) - 1].Pos
}

func (rf *Raft) getLastTerm() int {
	return rf.persitent_state.logs[len(rf.persitent_state.logs) - 1].Term
}

func (rf *Raft) GetLogState() (int, int) {
	return rf.getLastIndex(), rf.getLastTerm()
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.persitent_state.currentTerm)
	e.Encode(rf.persitent_state.votedFor)
	e.Encode(rf.persitent_state.logs)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	DPrintf("Reading From Persist")
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []Log
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil {
		log.Fatalf("%v, %v decode failed", time.Now(), rf.me)
	} else {
		rf.persitent_state.currentTerm = currentTerm
		rf.persitent_state.votedFor = votedFor
		rf.persitent_state.logs = logs

		// When a leader first comes to power,
		//it initializes all nextIndex values to the index just after the
		//last one in its log
		rf.volatile_leader.nextIndex = INITIALIZE(len(rf.peers), rf.getLastIndex() + 1)

		// (initialized to 0, increases monotonically)
		rf.volatile_leader.matchIndex = INITIALIZE(len(rf.peers), 0)
	}

}






//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()

	term := rf.persitent_state.currentTerm
	isLeader := rf.cur_state == Leader
	if (!isLeader) {
		rf.mu.Unlock()
		return -1, -1, isLeader
	}

	// Your code here (2B).
	new_log := Log{
		Command:      command,
		Term: rf.persitent_state.currentTerm,
		Pos: len(rf.persitent_state.logs),
	}
	DPrintf("Server: %d Recieved New Log: Term: %d, Pos: %d, Command: %d", rf.me, rf.persitent_state.currentTerm, len(rf.persitent_state.logs), command)
	rf.persitent_state.logs = append(rf.persitent_state.logs, new_log)
	lastIndex := rf.getLastIndex()
	rf.volatile_leader.matchIndex[rf.me] = lastIndex
	rf.volatile_leader.nextIndex[rf.me] = lastIndex + 1
	//DPrintf("Leader: %d Received New Entry. Current Log: %v", rf.me, rf.logs)
	rf.persist()
	rf.mu.Unlock()

	go rf.SendAppendEntries()

	return lastIndex, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.leader_Cond = sync.NewCond(&rf.mu)
	rf.apply_chan = applyCh
	// Your initialization code here (2A, 2B, 2C).
	rf.persitent_state.currentTerm = 0
	rf.cur_state = Follower
	rf.persitent_state.votedFor = -1
	rf.persitent_state.logs = []Log{{
		Command: nil,
		Term:    -1,
		Pos:     0,
	}}

	rf.volatile_all_server.lastApplied = 0
	rf.volatile_all_server.commitIndex = 0

	rf.volatile_leader.nextIndex =  INITIALIZE(len(rf.peers), rf.getLastIndex() + 1)
	rf.volatile_leader.matchIndex =  INITIALIZE(len(rf.peers), 0)

	rf.last_synced_time = time.Now()
	rf.time_out_period = rand.Intn(TimeOutEndRange - TimeOutStartRange) + TimeOutStartRange
	DPrintf("Server: %d has time out perioud: %d\n", rf.me, rf.time_out_period)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.BackgroundLeaderElectionActivationTrigger()
	go rf.SendAppliedMsg()

	return rf
}

// long running go roroutine that periodically check whether a raft server needs to start leader election process
func (rf *Raft) BackgroundLeaderElectionActivationTrigger() {
	for (!rf.killed()){

		//DPrintf("Server: %d DiffTime: %d comparing against: %d\n", rf.me, time.Now().Sub(rf.initializedTime).Milliseconds(), rf.timeOutPeriod)
		time.Sleep(time.Duration(rf.time_out_period) * time.Millisecond)

		rf.mu.Lock()
		if (rf.cur_state != Leader && (time.Now().Sub(rf.last_synced_time) > time.Duration(rf.time_out_period) * time.Millisecond || rf.persitent_state.votedFor == -1)) {
			//DPrintf("Server: %d DiffTime: %d comparing against: %d\n", rf.me, time.Now().Sub(rf.lastVisitedTime).Milliseconds(), rf.timeOutPeriod)
			//DPrintf("Server: %d Start DiffTime: %d comparing against: %d\n", rf.me, time.Now().Sub(rf.initializedTime).Milliseconds(), rf.timeOutPeriod)

			rf.mu.Unlock()


			DPrintf("Server: %d is starting leader election process\n", rf.me)
			go StartElectionProcess(rf)
		} else {
			rf.mu.Unlock()
		}

	}
}

// long running go routine to send heart beat information to peers, only send them when server is the leader
func (rf *Raft) BackgroundAppendEntries() {
	for (!rf.killed()){
		rf.mu.Lock()
		if (rf.cur_state != Leader) {
			rf.mu.Unlock()
			return
		}
		//DPrintf("Server: %d sending heart beat\n", rf.me)

		rf.mu.Unlock()

		go rf.SendAppendEntries()
		time.Sleep(time.Duration(HeartBeatInterval) * time.Millisecond)
	}
}

// must be called under protection of mutex
func (rf *Raft) UpdateCommit() {
	if rf.cur_state != Leader {
		return
	}

	n_peers := len(rf.peers)
	target_index := (n_peers - 1) / 2
	match_index_cpy := make([]int, n_peers)
	copy(match_index_cpy, rf.volatile_leader.matchIndex)
	DPrintf("From Leader: %d Matched Index: %v\n", rf.me, rf.volatile_leader.matchIndex)

	sort.Ints(match_index_cpy)

	majority_log_index := match_index_cpy[target_index]
	for i:= majority_log_index; i > rf.volatile_all_server.commitIndex; i-- {
		// only update commit index is it is of the same term as the current term
		if (rf.persitent_state.logs[i].Term == rf.persitent_state.currentTerm) {
			rf.volatile_all_server.commitIndex = i
			DPrintf("From Leader: %d Updated Commit Index: %v\n", rf.me, rf.volatile_all_server.commitIndex)
			break
		}
	}
}

func (rf *Raft) SendAppliedMsg() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.volatile_all_server.commitIndex == rf.volatile_all_server.lastApplied {
			rf.mu.Unlock()
			time.Sleep(time.Duration(50) * time.Millisecond)
			continue
		}
		cur_commitIndex := rf.volatile_all_server.commitIndex
		cur_lastApplied := rf.volatile_all_server.lastApplied
		rf.mu.Unlock()

		for cur_lastApplied < cur_commitIndex {
			cur_lastApplied++
			rf.apply_chan <- ApplyMsg{
				CommandValid: true,
				Command:      rf.persitent_state.logs[cur_lastApplied].Command,
				CommandIndex: rf.persitent_state.logs[cur_lastApplied].Pos,
			}
			//}
			DPrintf("Server: %v  Commit_ID: %d \n", rf.me, rf.volatile_all_server.commitIndex)
		}

		rf.mu.Lock()
		rf.volatile_all_server.lastApplied = cur_lastApplied
		rf.mu.Unlock()
	}
}


func (rf *Raft)  SendAppendEntriesHelper(server_idx int, request *AppendEntriesArgs) {
	reply := &AppendEntriesReply{}
	succeed := rf.sendAppendEntryRequest(server_idx, request, reply)

	if (succeed) {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if request.LeaderTerm != rf.persitent_state.currentTerm {
			return
		}
		rf.maybeUpdateTerm(reply.Term)
		if (rf.cur_state == Leader) {
			if (reply.Success) {
				rf.volatile_leader.nextIndex[server_idx] = request.PrevLogIndex + len(request.Entries) + 1

				// match index can only be monotomically increaseing
				rf.volatile_leader.matchIndex[server_idx] = MAX(rf.volatile_leader.matchIndex[server_idx], request.PrevLogIndex + len(request.Entries))
				rf.UpdateCommit()
			} else {
				// TODO: Implement optimization
				rf.volatile_leader.nextIndex[server_idx] = reply.NextIndex

			}
		}
	}

}

func (rf* Raft) SendAppendEntries()  {
	state_snapshot := rf.AppendEntryRequestSnapshot()
	for server_idx := 0; server_idx < len(rf.peers); server_idx++ {
		if server_idx == rf.me {
			continue
		}
		request := rf.AssembleAppendEntriesRequest(server_idx, state_snapshot)

		go rf.SendAppendEntriesHelper(server_idx, request)
	}
}

func StartElectionProcess(rf *Raft)  {
	rf.mu.Lock()
	DPrintf("Server: %d is in leader election process\n", rf.me)

	rf.cur_state = Candidate
	rf.persitent_state.currentTerm += 1
	rf.persitent_state.votedFor = rf.me
	rf.last_synced_time = time.Now()
	// reset random perioud at the start of election process
	rf.time_out_period = rand.Intn(TimeOutEndRange - TimeOutStartRange) + TimeOutStartRange
	//DPrintf("new timeout: %d\n", time.Duration(rf.timeOutPeriod) * time.Millisecond)
	rf.persist()
	rf.mu.Unlock()

	count := 1 // vote itself
	var wg sync.WaitGroup

	request := rf.AssembleVoteRequest()
	for server_idx := 0; server_idx < len(rf.peers); server_idx++ {
		if server_idx == rf.me {
			continue
		}
		wg.Add(1)
		go func(server_idx int) {
			reply := &RequestVoteReply{}

			succeed := rf.sendRequestVote(server_idx, request, reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if (!succeed || rf.persitent_state.currentTerm != request.Term) {
				wg.Done()
				return
			}

			if succeed && rf.cur_state == Candidate {
				if reply.VoteGranted {
					//DPrintf("vote granted from server: %d\n", server_idx)
					count += 1
				} else {
					rf.maybeUpdateTerm(reply.Term)
				}

				if (rf.cur_state == Candidate && count * 2 > len(rf.peers)) {
					rf.cur_state = Leader
					DPrintf("Elected New Leader: %d\n", rf.me)
					rf.volatile_leader.matchIndex = INITIALIZE(len(rf.peers), 0)
					rf.volatile_leader.nextIndex = INITIALIZE(len(rf.peers), rf.getLastIndex() + 1)
					rf.volatile_leader.matchIndex[rf.me] = rf.getLastIndex()
					go rf.BackgroundAppendEntries()
				}
			}
			wg.Done()
		}(server_idx)
	}

	wg.Wait()
}
