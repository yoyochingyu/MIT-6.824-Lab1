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
	"log"
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

func init() {
	rand.Seed(time.Now().UnixNano())
	log.SetFlags(log.Ltime | log.Lmicroseconds)
	//f, err := os.OpenFile("testlogfile", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0777)
	//if err != nil {
	//	log.Fatalf("error opening file: %v", err)
	//}
	//defer f.Close()
	//log.SetOutput(f)
}

// import "bytes"
// import "../labgob"

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

type LogEntry struct {
	Command interface{} // todo: uppercase
	Index   int
	Term    int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access (goroutine) to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// 2A
	state       State
	currentTerm int
	leaderId    int        // for redirect client requests
	votedFor    int        // candidateId that received my vote at this term
	log         []LogEntry // todo: ptr
	timerCh     chan ChanArg
	electCh     chan ChanArg
}

var NULL int = -1

type State string

var (
	Follower  State = "FOLLOWER"
	Candidate State = "CANDIDATE"
	Leader    State = "LEADER"
)

type ChanArg string

var (
	RestartTimer     ChanArg = "RESTART_TIMER"
	Timeout          ChanArg = "TIMEOUT"
	ChangeToLeader   ChanArg = "CHANGE_TO_LEADER"
	ChangeToFollower ChanArg = "CHANGE_TO_FOLLOWER"
	Vote             ChanArg = "VOTE"
)

// return currentTerm and whether this server
// believes it is the leader. // todo: check
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool

	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.me == rf.leaderId
	rf.mu.Unlock()

	return term, isleader
}

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

/******************************************************
** Request Vote
*****************************************************/
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int // todo: 2B
	LastLogTerm  int // todo: 2B
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) { // todo
	// Your code here (2A, 2B).
	term, candidateId, _, _ := args.Term, args.CandidateId, args.LastLogIndex, args.LastLogTerm

	rf.mu.Lock()
	if term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		log.Printf("[RefuseVote] server (id: %d) term > candidate (id: %d) term\n", rf.me, candidateId)
		rf.mu.Unlock()
		return
	}

	// change state
	if term > rf.currentTerm {
		if rf.state == Follower {
			rf.changeToFollower(term) // todo: log
			rf.votedFor = NULL
			rf.timerCh <- RestartTimer
			// todo: vote/checkvote
		} else if rf.state == Candidate || rf.state == Leader {
			rf.changeToFollower(term)
			rf.votedFor = NULL
			go rf.monitorHeartbeat() // todo: will this have the ones remaining when last time as follower?

			// todo: vote/checkvote
		}

	} else {
		if rf.state == Follower {
			rf.timerCh <- RestartTimer // todo: candidate, leader don't have this?
		}
	}
	// check can I vote this guy
	if rf.votedFor == NULL || rf.votedFor == candidateId {
		rf.votedFor = candidateId
		reply.VoteGranted = true
		log.Printf("[GrantVote] server (id: %d) votes for candidate (id: %d)\n", rf.me, candidateId)
	} else { // todo: check
		reply.VoteGranted = false
		log.Printf("[RefuseVote] server (id: %d) already votes for term: %d\n", rf.me, rf.currentTerm)
	}
	// todo: leader may recv!
	//todo: votedFor set as null!
	// todo: has to issueHB and ReqVote to self

	// original
	//if term > rf.currentTerm {
	//	// restart timer
	//	rf.timerCh <- RestartTimer // todo: candidate, leader on't have this?
	//
	//	rf.changeToFollower(term)
	//	//go rf.monitorHeartbeat() //todo: prob?
	//
	//	rf.votedFor = candidateId
	//	reply.VoteGranted = true
	//	log.Printf("[Server %d] votes for %d\n", rf.me, candidateId)
	//} else {
	//	// restart timer
	//	rf.timerCh <- RestartTimer // todo: candidate, leader don't have this?
	//
	//	if rf.votedFor == NULL || rf.votedFor == candidateId { // todo: leader?
	//		rf.votedFor = candidateId
	//		reply.VoteGranted = true
	//		log.Printf("[Server %d] votes for %d\n", rf.me, candidateId)
	//	} else {
	//		reply.VoteGranted = false
	//	}
	//
	//}
	rf.mu.Unlock()
}

/******************************************************
** AppendEntries
*****************************************************/

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry // todo: ptr?
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// sendAppendEntries make RPC call to ask peers to append entries
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// todo: votedFor
// face to leader(self), follower, candidate
// AppendEntries is the handler for sendAppendEntries
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	term, leaderId, _, _, _, _ := args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.Entries, args.LeaderCommit

	rf.mu.Lock()

	if term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}
	if term == rf.currentTerm && rf.state == Leader {
		reply.Success = true
		rf.mu.Unlock()
		return
	}

	if rf.state == Follower {
		rf.timerCh <- RestartTimer
	} else if rf.state == Candidate {
		//todo: does this implicityly means voting to this leader????????? (set votedFor)
		rf.votedFor = leaderId         //todo:?????????
		go rf.monitorHeartbeat()       // todo: will this have the ones remaining when last time as follower?
		rf.electCh <- ChangeToFollower // use to shut down startElection goroutine
	} else if rf.state == Leader {
		rf.votedFor = leaderId   //todo:?????????
		go rf.monitorHeartbeat() // todo: will this have the ones remaining when last time as follower?
	}
	rf.changeToFollower(term)
	rf.leaderId = leaderId
	reply.Success = true
	// detailed version
	//if term > rf.currentTerm{
	//	if rf.state == Follower{
	//		rf.changeToFollower(term)
	//		rf.leaderId = leaderId
	//		rf.timerCh <- RestartTimer
	//	}else if rf.state == Candidate{
	//		rf.changeToFollower(term)
	//		rf.leaderId = leaderId
	//		go rf.monitorHeartbeat() // todo: will this have the ones remaining when last time as follower?
	//	}
	//	// leader won't send AE to self
	//}else if term == rf.currentTerm{
	//	if rf.state == Follower{
	//		rf.leaderId = leaderId
	//		rf.timerCh <- RestartTimer
	//	}else if rf.state == Candidate{
	//		rf.changeToFollower(term)
	//		rf.leaderId = leaderId
	//		go rf.monitorHeartbeat() // todo: will this have the ones remaining when last time as follower?
	//	}
	// todo: leader won't send AE to self
	//}
	//

	// original version
	//if term > rf.currentTerm || rf.state == Candidate {
	//	rf.changeToFollower(term)
	//}
	//if rf.state != Leader {
	//	rf.leaderId = leaderId
	//	rf.timerCh <- RestartTimer // todo: position? leader/follower
	//}
	rf.mu.Unlock()
}

// changeToFollower updates term, set state follower, . requires locking mechanism
// todo: timer restart timer
func (rf *Raft) changeToFollower(term int) {
	log.Printf("[System] server (id: %d) state: %s, term: %d->%d\n", rf.me, Follower, rf.currentTerm, term)
	//log.Printf("[System] server (id: %d) state: %s -> %s, term: %d->%d\n", rf.me, "RE",Follower, rf.currentTerm, term, )
	rf.currentTerm = term
	//rf.votedFor = NULL // shouldn't,
	rf.state = Follower

}

func (rf *Raft) issueOneHeartbeat(i int) {
	// wrap arg, reply
	rf.mu.Lock()
	arg := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: NULL,
		PrevLogTerm:  NULL,
		Entries:      []LogEntry{},
		LeaderCommit: NULL,
	}
	rf.mu.Unlock()
	reply := AppendEntriesReply{}

	// send heartbeat
	log.Printf("[IssueHB] leader (id: %d) issue heatbeat to %d\n", rf.me, i)
	rf.sendAppendEntries(i, &arg, &reply)

	// handle response
	term, success := reply.Term, reply.Success

	if !success {
		rf.mu.Lock()
		if term > rf.currentTerm {
			log.Printf("[IssueHB] leader (id: %d) resp term > current term, go back to follower", rf.me)
			rf.changeToFollower(term)
			rf.votedFor = NULL
			go rf.monitorHeartbeat()
		} else {
			// reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) issueHeartbeat() {
	rf.mu.Lock()
	rf.leaderId = rf.me
	rf.state = Leader
	rf.mu.Unlock()

	for rf.state == Leader && !rf.killed() {
		log.Printf("[IssueHB] leader (id: %d) issuing heatbeat\n", rf.me)
		for i := range rf.peers {
			go rf.issueOneHeartbeat(i)
		}
		time.Sleep(100 * time.Millisecond)
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
	log.Printf("%d has been killed, isleader:%t", rf.me, rf.me == rf.leaderId)
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

	// Your initialization code here (2A, 2B, 2C).
	rf.mu = sync.Mutex{} // todo: ptr
	rf.state = Follower
	rf.currentTerm = 0
	rf.leaderId = NULL //todo: may have problem if client request come at this time
	rf.votedFor = NULL
	rf.timerCh = make(chan ChanArg)
	rf.electCh = make(chan ChanArg, len(rf.peers)+1)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	log.Printf("[System] server (id: %d) created, term: %d\n", rf.me, rf.currentTerm)
	go rf.monitorHeartbeat()
	return rf
}

func (rf *Raft) startTimer(quit chan ChanArg, ch chan ChanArg) {
	dur := time.Duration(rand.Intn(200)+200) * time.Millisecond
	time.Sleep(dur)
	select {
	case <-quit:
		return
	default:
		ch <- Timeout // todo: ptr
	}
}

func (rf *Raft) monitorHeartbeat() {
	//if rf.killed() {
	//	return
	//}
	// 2A
	log.Printf("[MonitorHB] server (id: %d) starts monitor heartbeat\n", rf.me)
	quit := make(chan ChanArg, 1)
	go rf.startTimer(quit, rf.timerCh)

	for !rf.killed() {
		arg := <-rf.timerCh
		if arg == RestartTimer {
			quit <- "QUIT"
			log.Printf("[MonitorHB] server (id: %d) recv RPC, restart timer", rf.me)
			quit = make(chan ChanArg, 1)
			go rf.startTimer(quit, rf.timerCh)
		} else {
			log.Printf("[MonitorHB] server (id: %d) timeout, changed to candidate and start election", rf.me)
			go rf.startElection()
			return
		}
	}
}

func (rf *Raft) issueOneReqVote(i int) {

	// wrap arg, reply,
	rf.mu.Lock()
	arg := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: NULL,
		LastLogTerm:  NULL,
	}
	rf.mu.Unlock()
	reply := RequestVoteReply{}

	// send reqVote
	log.Printf("[Election] server (id: %d) ask server (id: %d) to vote, term: %d", rf.me, i, rf.currentTerm)
	rf.sendRequestVote(i, &arg, &reply)

	// handle response
	term, voteGranted := reply.Term, reply.VoteGranted
	rf.mu.Lock()
	if rf.state == Candidate {
		if !voteGranted && term > rf.currentTerm {
			rf.changeToFollower(term)
			rf.votedFor = NULL
			go rf.monitorHeartbeat()
			rf.electCh <- ChangeToFollower // use to shut down startElection goroutine
		} else if voteGranted {
			rf.electCh <- Vote
		}
	}
	rf.mu.Unlock()
}

func (rf *Raft) startElection() {
	for !rf.killed() {
		// update term, vote for self, change to candidate
		rf.mu.Lock()
		rf.state = Candidate
		rf.currentTerm = rf.currentTerm + 1
		rf.votedFor = rf.me
		// clear electCh, to prevent counting votes from previous round // todo
		rf.electCh = make(chan ChanArg, len(rf.peers)+1)
		rf.mu.Unlock()

		// fire timer
		quit := make(chan ChanArg, 1)
		go rf.startTimer(quit, rf.electCh)

		// fire reqVote
		for i := range rf.peers {
			go rf.issueOneReqVote(i) // todo: rename
		}

		// wait for signal
		votes := 0
		for arg := range rf.electCh {
			if arg == Vote { // todo: rename
				votes++
				if votes == len(rf.peers)/2+1 {
					quit <- "QUIT"
					log.Printf("[WinElection] server %d change to be leader\n", rf.me)
					go rf.issueHeartbeat()
					return
				}
			} else if arg == Timeout {
				log.Printf("[Election] server (id: %d)election timeout, restart election\n", rf.me)
				break
			} else if arg == ChangeToFollower {
				return
			}
		}
	}

}
