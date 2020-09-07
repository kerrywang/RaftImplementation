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
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

// import "bytes"
// import "../labgob"

type StateType string
const (
	Candidate StateType = "Candidate"
	Follower = "Follower"
	Leader = "Leader"
)

const TimeOutStartRange = 400
const TimeOutEndRange = 1000
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

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm int
	votedFor int
	logs   	[]Log
	curState StateType

	lastVisitedTime time.Time
	timeOutPeriod int
}

type Log struct {
	command []byte
	recievedTerm int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int = rf.currentTerm
	var isleader bool = rf.curState == Leader
	// Your code here (2A).
	return term, isleader
}

func (rf *Raft) GetLogState() (int, int) {
	var lastLogIndex int = len(rf.logs) - 1
	var lastLogTerm int = 0
	if (lastLogIndex >= 0) {
		lastLogTerm = rf.logs[lastLogIndex].recievedTerm
	}
	return lastLogIndex, lastLogTerm
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
	curLogTerm, curLogIndex := rf.GetLogState()


	request_vote.LastLogTerm = curLogTerm
	request_vote.Term = rf.currentTerm
	request_vote.CandidateId = rf.me
	request_vote.LastLogIndex = curLogIndex

	return request_vote
}

func (rf *Raft) AssembleAppendEntriesRequest() (*AppendEntriesArgs)  {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	append_entry_request := &AppendEntriesArgs{}
	append_entry_request.LeaderId = rf.me
	append_entry_request.LeaderTerm = rf.currentTerm
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


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

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.curState = Follower
	rf.votedFor = -1

	rf.lastVisitedTime = time.Now()
	rf.timeOutPeriod = rand.Intn(TimeOutEndRange - TimeOutStartRange) + TimeOutStartRange
	//fmt.Printf("Server: %d has time out perioud: %d\n", rf.me, rf.timeOutPeriod)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go BackgroundLeaderElectionActivationTrigger(rf)
	//go BackgroundHeartBeatSender(rf)rf
	return rf
}

// long running go roroutine that periodically check whether a raft server needs to start leader election process
func BackgroundLeaderElectionActivationTrigger(rf *Raft) {
	for (!rf.killed()){
		rf.mu.Lock()
		if (rf.curState != Leader && time.Now().Sub(rf.lastVisitedTime) > time.Duration(rf.timeOutPeriod)*  time.Millisecond) {
			rf.mu.Unlock()
			//fmt.Printf("RealTime: %d Current Time: %d comparing against: %d\n", time.Now().Second(), time.Now().Sub(rf.lastVisitedTime), time.Duration(rf.timeOutPeriod)*  time.Millisecond)
			//fmt.Printf("Server: %d is starting leader election process\n", rf.me)
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
		//fmt.Printf("Server: %d sending heart beat\n", rf.me)

		rf.mu.Unlock()
		go SendHeartBeat(rf)
		time.Sleep(time.Duration(HeartBeatInterval) * time.Millisecond)
	}
}

func SendHeartBeat(rf *Raft)  {
	for server_idx := 0; server_idx < len(rf.peers); server_idx++ {
		if server_idx == rf.me {
			continue
		}

		go func(server_idx int) {
			reply := &AppendEntriesReply{}
			request := rf.AssembleAppendEntriesRequest()

			succeed := rf.sendAppendEntryRequest(server_idx, request, reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if succeed && rf.currentTerm < reply.Term {
				rf.curState = Follower
				rf.currentTerm = reply.Term
			}
		}(server_idx)
	}
}

func StartElectionProcess(rf *Raft)  {
	rf.mu.Lock()
	//fmt.Printf("Server: %d is in leader election process\n", rf.me)

	rf.curState = Candidate
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.lastVisitedTime = time.Now()
	// reset random perioud at the start of election process
	rf.timeOutPeriod = rand.Intn(TimeOutEndRange - TimeOutStartRange) + TimeOutStartRange
	//fmt.Printf("new timeout: %d\n", time.Duration(rf.timeOutPeriod) * time.Millisecond)

	rf.mu.Unlock()

	count := 1 // vote itself
	var wg sync.WaitGroup

	for server_idx := 0; server_idx < len(rf.peers); server_idx++ {
		if server_idx == rf.me {
			continue
		}
		wg.Add(1)
		go func(server_idx int) {
			reply := &RequestVoteReply{}
			request := rf.AssembleVoteRequest()

			succeed := rf.sendRequestVote(server_idx, request, reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if succeed && rf.curState == Candidate {
				if reply.VoteGranted {
					//fmt.Printf("vote granted from server: %d\n", server_idx)
					count += 1
				} else if (reply.Term > rf.currentTerm) {
					//fmt.Printf("Server: %d (Term: %d) abort leader election process because it recieves higer term %d", rf.me, rf.currentTerm, reply.Term)
					rf.curState = Follower
					rf.currentTerm = reply.Term
					rf.votedFor = -1
				}

				if (rf.curState == Candidate && count * 2 > len(rf.peers)) {
					rf.curState = Leader
					go BackgroundHeartBeatSender(rf)
				}
			}
			wg.Done()
		}(server_idx)
	}

	//for {
	//	rf.mu.Lock()
	//	if  (count * 2 > len(rf.peers) || rf.curState != Candidate) {
	//		break
	//	}
	//	rf.mu.Unlock()
	//	time.Sleep(time.Duration(50) *  time.Millisecond) // magic numebr here, wait for an arbitaray time
	//}
	//
	//// elected as leader
	//if (count * 2 > len(rf.peers)) {
	//	fmt.Printf("Elected a leader: %d\n", rf.me)
	//	rf.curState = Leader
	//	rf.leader_Cond.Signal()
	//} else {
	//	rf.curState = Follower
	//}
	//rf.mu.Unlock()
	wg.Wait()
}
