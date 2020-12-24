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
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

import "../labgob"

type AppendEntryMode string
const (
	HeartBeatMode AppendEntryMode = "HeartBeat"
	AppendRequestMode AppendEntryMode = "AppendRequest"
)

type StateType string
const (
	Candidate StateType = "Candidate"
	Follower = "Follower"
	Leader = "Leader"
)

const TimeOutStartRange = 600
const TimeOutEndRange = 1500
const HeartBeatInterval = 100
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

func (rf *Raft) maybeUpdateTerm(term int) {
	if (term > rf.persitent_state.currentTerm) {
		rf.persitent_state.currentTerm = term
		rf.cur_state = Follower
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

func (rf *Raft) HasLaterLog(lastLogIndex int, lastRecievedTerm int) (bool){
	curLastLogIndex, curLastLogTerm := rf.GetLogState()

	if (curLastLogIndex != -1 && lastLogIndex != -1 && curLastLogTerm != lastRecievedTerm) {
		return curLastLogTerm > lastRecievedTerm
	} else {
		return curLastLogIndex > lastLogIndex
	}
}

func (rf *Raft) AssembleVoteRequest() (*RequestVoteArgs)  {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	request_vote := &RequestVoteArgs{}
	curLogIndex, curLogTerm  := rf.GetLogState()


	request_vote.LastLogTerm = curLogTerm
	request_vote.Term = rf.persitent_state.currentTerm
	request_vote.CandidateId = rf.me
	request_vote.LastLogIndex = curLogIndex

	rf.persist()
	return request_vote
}

func (rf *Raft) AssembleAppendEntriesRequest(mode AppendEntryMode, server_id int) (*AppendEntriesArgs)  {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	append_entry_request := &AppendEntriesArgs{}
	append_entry_request.LeaderId = rf.me
	append_entry_request.LeaderTerm = rf.persitent_state.currentTerm
	append_entry_request.LeaderCommit = rf.volatile_all_server.commitIndex
	append_entry_request.PrevLogIndex = rf.volatile_leader.nextIndex[server_id] - 1
	append_entry_request.PrevLogTerm = rf.persitent_state.logs[append_entry_request.PrevLogIndex].Term
	//DPrintf("prev log term: %d has log: %v\n", append_entry_request.PrevLogTerm, rf.logs)

	if (mode == AppendRequestMode) {
		append_entry_request.Entries = rf.logs[rf.nextIndex[server_id]: ]
	}

	rf.persist()
	return append_entry_request
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
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)

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
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
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
	defer rf.mu.Unlock()

	term := rf.currentTerm
	isLeader := rf.curState == Leader
	if (!isLeader) {
		return -1, -1, isLeader
	}

	// Your code here (2B).
	new_log := Log{
		Command:      command,
		Term: rf.currentTerm,
		Pos: len(rf.logs),
	}

	rf.logs = append(rf.logs, new_log)
	index, _ := rf.GetLogState()
	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me] = index + 1
	//DPrintf("Leader: %d Received New Entry. Current Log: %v", rf.me, rf.logs)
	go SendAppendEntries(rf, AppendRequestMode)

	return index, term, isLeader
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
	rf.currentTerm = 0
	rf.curState = Follower
	rf.votedFor = -1
	rf.logs = []Log{{
		Command: nil,
		Term:    -1,
		Pos:     0,
	}}

	rf.lastApplied = 0
	rf.commitIndex = 0

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.lastVisitedTime = time.Now()
	rf.initializedTime = time.Now()
	rf.timeOutPeriod = rand.Intn(TimeOutEndRange - TimeOutStartRange) + TimeOutStartRange
	DPrintf("Server: %d has time out perioud: %d\n", rf.me, rf.timeOutPeriod)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go BackgroundLeaderElectionActivationTrigger(rf)
	go rf.SendAppliedMsg()
	//go BackgroundHeartBeatSender(rf)rf
	return rf
}

// long running go roroutine that periodically check whether a raft server needs to start leader election process
func BackgroundLeaderElectionActivationTrigger(rf *Raft) {
	for (!rf.killed()){
		rf.mu.Lock()

		//DPrintf("Server: %d DiffTime: %d comparing against: %d\n", rf.me, time.Now().Sub(rf.initializedTime).Milliseconds(), rf.timeOutPeriod)

		if (rf.curState != Leader && time.Now().Sub(rf.lastVisitedTime) > time.Duration(rf.timeOutPeriod)*  time.Millisecond ) {
			//DPrintf("Server: %d DiffTime: %d comparing against: %d\n", rf.me, time.Now().Sub(rf.lastVisitedTime).Milliseconds(), rf.timeOutPeriod)
			//DPrintf("Server: %d Start DiffTime: %d comparing against: %d\n", rf.me, time.Now().Sub(rf.initializedTime).Milliseconds(), rf.timeOutPeriod)

			rf.mu.Unlock()


			//DPrintf("Server: %d is starting leader election process\n", rf.me)
			go StartElectionProcess(rf)
		} else {
			rf.mu.Unlock()
		}


		time.Sleep(time.Duration(rf.timeOutPeriod) * time.Millisecond)
	}
}

// long running go routine to send heart beat information to peers, only send them when server is the leader
func BackgroundHeartBeatSender(rf *Raft) {
	for (!rf.killed()){
		rf.mu.Lock()
		for (rf.curState != Leader) {
			rf.mu.Unlock()
			return
		}
		//DPrintf("Server: %d sending heart beat\n", rf.me)

		rf.mu.Unlock()
		go SendAppendEntries(rf, HeartBeatMode)
		time.Sleep(time.Duration(HeartBeatInterval) * time.Millisecond)
	}
}

func (rf *Raft) SendHeardBeatHelper(server_idx int) {
	reply := &AppendEntriesReply{}
	request := rf.AssembleAppendEntriesRequest(HeartBeatMode, server_idx)
	succeed := rf.sendAppendEntryRequest(server_idx, request, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if succeed && rf.currentTerm < reply.Term {
		rf.curState = Follower
		rf.currentTerm = reply.Term
		rf.votedFor = -1
	}
}

func (rf *Raft) SendAppliedMsg() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.commitIndex == rf.lastApplied {
			rf.mu.Unlock()
			time.Sleep(time.Duration(50) * time.Millisecond)
			continue
		}
		cur_commitIndex := rf.commitIndex
		cur_lastApplied := rf.lastApplied
		rf.mu.Unlock()

		for cur_lastApplied < cur_commitIndex {
			rf.apply_chan <- ApplyMsg{
				CommandValid: true,
				Command:      rf.logs[cur_lastApplied].Command,
				CommandIndex: rf.logs[cur_lastApplied].Pos,
			}
			cur_lastApplied++
			//}
			DPrintf("Server: %v  [LEADER] Commit_ID: %d\n", rf.me, rf.commitIndex)
		}

		rf.mu.Lock()
		rf.lastApplied = cur_lastApplied
		rf.mu.Unlock()
	}
}


func (rf *Raft) SendAppendEntriesHelper(server_idx int) {
	should_finish := false
	mode := AppendRequestMode
	for !should_finish {
		reply := &AppendEntriesReply{}
		request := rf.AssembleAppendEntriesRequest(mode, server_idx)

		// make sure the request is still up-to-date. If the internal state changed restart the query
		rf.mu.Lock()
		if rf.currentTerm != request.LeaderTerm || rf.curState != Leader {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		succeed := rf.sendAppendEntryRequest(server_idx, request, reply)

		rf.mu.Lock()

		if !succeed {
			should_finish = rf.curState != Leader
			rf.mu.Unlock()
			continue
		}

		if rf.currentTerm != request.LeaderTerm || rf.curState != Leader {
			rf.mu.Unlock()
			return
		}

		if rf.currentTerm < reply.Term {
			rf.curState = Follower
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.persist()
		} else if (!reply.Success) {
			rf.nextIndex[server_idx] =  reply.NextIndex
		} else if (len(request.Entries) != 0) {
			//DPrintf("Request Succeed Return from: %d", server_idx)

			rf.nextIndex[server_idx] = request.Entries[len(request.Entries) - 1].Pos + 1
			rf.matchIndex[server_idx] = rf.nextIndex[server_idx] - 1
			rf.updateCommitIndex()
		}
		should_finish = reply.Success || rf.curState != Leader
		rf.mu.Unlock()

	}
}

//func (rf * Raft) BackGroundApplyMsg() {
//
//}

func SendAppendEntries(rf *Raft, mode AppendEntryMode)  {

	for server_idx := 0; server_idx < len(rf.peers); server_idx++ {
		if server_idx == rf.me {
			continue
		}

		go func(server_idx int) {
			switch mode {
			case HeartBeatMode:
				rf.SendHeardBeatHelper(server_idx)
			case AppendRequestMode:
				rf.SendAppendEntriesHelper(server_idx)
			}
		}(server_idx)
	}
}

func StartElectionProcess(rf *Raft)  {
	rf.mu.Lock()
	DPrintf("Server: %d is in leader election process\n", rf.me)

	rf.curState = Candidate
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.lastVisitedTime = time.Now()
	// reset random perioud at the start of election process
	rf.timeOutPeriod = rand.Intn(TimeOutEndRange - TimeOutStartRange) + TimeOutStartRange
	//DPrintf("new timeout: %d\n", time.Duration(rf.timeOutPeriod) * time.Millisecond)

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
			if (rf.currentTerm != request.Term) {
				return
			}

			if succeed && rf.curState == Candidate {
				if reply.VoteGranted {
					//DPrintf("vote granted from server: %d\n", server_idx)
					count += 1
				} else if (reply.Term > rf.currentTerm) {
					//DPrintf("Server: %d (Term: %d) abort leader election process because it recieves higer term %d", rf.me, rf.currentTerm, reply.Term)
					rf.curState = Follower
					rf.currentTerm = reply.Term
					rf.votedFor = -1
				}

				if (rf.curState == Candidate && count * 2 > len(rf.peers)) {
					rf.curState = Leader
					DPrintf("Elected New Leader: %d\n", rf.me)
					for peer_id := 0; peer_id < len(rf.peers); peer_id++ {
						rf.nextIndex[peer_id] = len(rf.logs)
						rf.matchIndex[peer_id] = 0
					}
					go BackgroundHeartBeatSender(rf)
				}
			}
			wg.Done()
		}(server_idx)
	}

	wg.Wait()
}
