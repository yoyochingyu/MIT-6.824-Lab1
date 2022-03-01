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
	"fmt"
	"log"
	"math"
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
	leaderCh    chan ChanArg

	// 2B
	commitIdx   int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
	applyCh     chan ApplyMsg
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
	ChangeToFollower ChanArg = "CHANGE_TO_FOLLOWER"
	Vote             ChanArg = "VOTE"
	QuitTimer        ChanArg = "QUIT_TIMER"
)

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
	// 2A
	rf.mu = sync.Mutex{} // todo: ptr
	rf.state = Follower
	rf.currentTerm = 0
	rf.leaderId = NULL // todo:need?
	rf.votedFor = NULL
	rf.timerCh = make(chan ChanArg)                  // todo: rename
	rf.electCh = make(chan ChanArg, len(rf.peers)+1) // +1: space for timer arg
	rf.leaderCh = make(chan ChanArg)                 // todo: rename

	// 2B
	rf.commitIdx = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh
	// log starts at index: 1, insert empty logEntry at index: 0
	rf.log = append(rf.log, LogEntry{Index: 0, Term: NULL, Command: NULL})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	log.Printf("[System] server (id: %d) created, term: %d\n", rf.me, rf.currentTerm)
	go rf.monitorHeartbeat()
	return rf
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
func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	rf.mu.Lock()
	index = len(rf.log)
	term = rf.currentTerm
	isLeader = rf.state == Leader
	rf.mu.Unlock()
	if isLeader {
		rf.mu.Lock() // todo:????
		rf.log = append(rf.log, LogEntry{Index: index, Term: term, Command: command})
		rf.mu.Unlock()
		log.Printf("[System] leader (id: %d) recv client req, append to log index: %d, command: %v\n", rf.me, index, command)
		rf.leaderCh <- RestartTimer
	}
	return
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
** RPC: Request Vote
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
	term, candId, candlastLogIndex, candlastLogTerm := args.Term, args.CandidateId, args.LastLogIndex, args.LastLogTerm

	rf.mu.Lock()
	if term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		log.Printf("[RefuseVote] server (id: %d) term > candidate (id: %d) term\n", rf.me, candId)
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
			if rf.state == Candidate {
				rf.electCh <- ChangeToFollower
			} else {
				rf.leaderCh <- ChangeToFollower
			}
			rf.changeToFollower(term)
			rf.votedFor = NULL
			go rf.monitorHeartbeat() // todo: will this have the ones remaining when last time as follower?
		}
	} else {
		if rf.state == Follower {
			rf.timerCh <- RestartTimer
		}
		// leader and candidate can just refuse voting
	}
	// check whether I can vote this guy
	isCandidateUptoDate := false
	lastLogIndex := len(rf.log) - 1
	if candlastLogTerm != rf.log[lastLogIndex].Term {
		if candlastLogTerm >= rf.log[lastLogIndex].Term {
			isCandidateUptoDate = true
		}
	} else {
		if candlastLogIndex >= lastLogIndex {
			isCandidateUptoDate = true
		}
	}

	if (rf.votedFor == NULL || rf.votedFor == candId) && isCandidateUptoDate {
		rf.votedFor = candId
		reply.VoteGranted = true
		log.Printf("[GrantVote] server (id: %d) votes for candidate (id: %d), votedFor: %v, isCandidateUpToDate: %t\n", rf.me, candId, rf.votedFor, isCandidateUptoDate)
	} else {
		reply.VoteGranted = false
		log.Printf("[RefuseVote] server (id: %d) refuse vote to candidate (id: %d)\n", rf.me, candId)
	}
	// todo: votedFor set as null!

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
** RPC: AppendEntries
*****************************************************/

type AppendEntriesArgs struct {
	Term         int        `json:"term"`
	LeaderId     int        `json:"leaderId"`
	PrevLogIndex int        `json:"prevLogIndex"`
	PrevLogTerm  int        `json:"prevLogTerm"`
	Entries      []LogEntry `json:"entries"` // todo: ptr?
	LeaderCommit int        `json:"leaderCommit"`
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
// new ver
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit := args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.Entries, args.LeaderCommit
	log.Printf("[AE] server (id: %d) recv AE RPC from leader (id: %d)", rf.me, leaderId)
	rf.mu.Lock()
	if term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}
	if term == rf.currentTerm && rf.state == Leader {
		reply.Success = true //todo: check
		rf.mu.Unlock()
		return
	}

	// change state, update term, restart timer, start monitorHB
	if rf.state == Follower {
		rf.timerCh <- RestartTimer
	} else if rf.state == Candidate {
		//todo: does this implicityly means voting to this leader????????? (set votedFor)
		rf.votedFor = leaderId         //todo:?????????
		go rf.monitorHeartbeat()       // todo: will this have the ones remaining when last time as follower?
		rf.electCh <- ChangeToFollower // use to shut down startElection goroutine
	} else if rf.state == Leader {
		rf.votedFor = leaderId          //todo:?????????
		go rf.monitorHeartbeat()        // todo: will this have the ones remaining when last time as follower?
		rf.leaderCh <- ChangeToFollower // use to shut down startElection goroutine todo: leaderCh shut down
	}
	rf.changeToFollower(term)
	rf.leaderId = leaderId
	rf.mu.Unlock()

	// 2B:
	// if isHeartbeat
	if len(entries) == 0 {
		reply.Success = true
	} else {
		rf.mu.Lock()
		if prevLogIndex >= len(rf.log) || rf.log[prevLogIndex].Term != prevLogTerm {
			log.Printf("[AE] server (id: %d) discover log inconsistency, refuse...\n", rf.me)
			reply.Success = false
		} else {
			// if new one conflicts with existing, delete existing (assume entries sorted with ascending order)
			insertIndex := entries[0].Index
			if len(rf.log) > insertIndex {
				rf.log = rf.log[:insertIndex]
			}
			rf.log = append(rf.log, entries...)
			log.Printf("[AE] server (id: %d) append log, current log as %v\n", rf.me, rf.log)
			reply.Success = true
		}
		rf.mu.Unlock()
	}

	// update commitIndex // todo:check
	rf.mu.Lock()
	if leaderCommit > rf.commitIdx {
		min := math.Min(float64(leaderCommit), float64(rf.log[len(rf.log)-1].Index))
		rf.commitIdx = int(min)
		log.Printf("[AE] server (id: %d) update commitIdx as %d\n", rf.me, rf.commitIdx)
		// apply to state machine
		if rf.commitIdx > rf.lastApplied {
			start := rf.lastApplied + 1
			end := rf.commitIdx + 1
			for i := start; i < end; i++ {
				logEntry := rf.log[i]
				rf.applyCh <- ApplyMsg{Command: logEntry.Command, CommandIndex: i, CommandValid: true}
				rf.lastApplied++
			}
			log.Printf("[AE] server (id: %d) applies index: %d~%d, updates lastApplied to %d\n", rf.me, start, end-1, rf.lastApplied)
		}
		fmt.Println(rf.log)
	}
	rf.mu.Unlock()

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

}

/******************************************************
** Helper
*****************************************************/
// changeToFollower updates term, set state follower, . requires locking mechanism
// todo: timer restart timer
func (rf *Raft) changeToFollower(term int) {
	log.Printf("[System] server (id: %d) state: %s, term: %d->%d\n", rf.me, Follower, rf.currentTerm, term)
	rf.currentTerm = term
	//rf.votedFor = NULL // shouldn't,
	rf.state = Follower

}

func (rf *Raft) startTimer(quit chan ChanArg, ch chan ChanArg, isHeartbeat bool) {
	var dur time.Duration
	if isHeartbeat {
		dur = 100 * time.Millisecond
	} else {
		dur = time.Duration(rand.Intn(200)+200) * time.Millisecond
	}
	time.Sleep(dur)

	select {
	case <-quit:
		return
	default:
		ch <- Timeout // todo: ptr
	}
}

/******************************************************
** Follower: monitorHB
*****************************************************/
func (rf *Raft) monitorHeartbeat() {
	//if rf.killed() {
	//	return
	//}
	// 2A
	rf.timerCh = make(chan ChanArg) // todo: rename
	log.Printf("[MonitorHB] server (id: %d) starts monitor heartbeat\n", rf.me)
	quit := make(chan ChanArg, 1)
	go rf.startTimer(quit, rf.timerCh, false)

	for !rf.killed() {
		arg := <-rf.timerCh
		if arg == RestartTimer {
			quit <- QuitTimer
			log.Printf("[MonitorHB] server (id: %d) recv RPC, restart timer", rf.me)
			quit = make(chan ChanArg, 1)
			go rf.startTimer(quit, rf.timerCh, false)
		} else {
			log.Printf("[MonitorHB] server (id: %d) timeout, changed to candidate and start election", rf.me)
			go rf.startElection()
			return
		}
	}
}

/******************************************************
** Candidate: startElection
*****************************************************/

func (rf *Raft) issueOneReqVote(i int) {

	// wrap arg, reply,
	rf.mu.Lock()
	if rf.state != Candidate {
		rf.mu.Unlock()
		return
	}
	lastLogIndex := len(rf.log) - 1
	arg := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  rf.log[lastLogIndex].Term,
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
		go rf.startTimer(quit, rf.electCh, false)

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
					quit <- QuitTimer
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

/******************************************************
** Leader: issueHB
*****************************************************/
func (rf *Raft) issueOneHeartbeat(i int, isHeartbeat bool) { // todo: create AEType enum
	// wrap arg, ply
	rf.mu.Lock() // tod
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	nextIndex := rf.nextIndex[i]
	prevLogIndex := rf.nextIndex[i] - 1
	arg := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  rf.log[prevLogIndex].Term,
		//Entries:      rf.log[rf.nextIndex[i]:], // todo
		LeaderCommit: rf.commitIdx,
	}
	//fmt.Printf("%+v\n", arg)
	if isHeartbeat {
		arg.Entries = []LogEntry{}
		log.Printf("[IssueHB] leader (id: %d) issue heatbeat to %d\n", rf.me, i)
	} else {
		arg.Entries = rf.log[rf.nextIndex[i]:]
		log.Printf("[IssueHB] leader (id: %d) issue AE to %d\n", rf.me, i)
	}

	rf.mu.Unlock()
	reply := AppendEntriesReply{}

	// send heartbeat
	// todo: if heartbeat lost, shall we retry?
	ok := rf.sendAppendEntries(i, &arg, &reply)
	// network error => do nothing? retry? todo
	if !ok {
		if !isHeartbeat {
			go rf.issueOneHeartbeat(i, isHeartbeat) // <----todo
		}
		return
	}
	// handle response
	term, success := reply.Term, reply.Success
	if !success {
		rf.mu.Lock()
		if term > rf.currentTerm {
			if rf.state == Leader {
				log.Printf("[IssueHB] leader (id: %d) resp term > current term, go back to follower", rf.me)
				rf.changeToFollower(term)
				rf.votedFor = NULL
				go rf.monitorHeartbeat()
				rf.leaderCh <- ChangeToFollower
			} else {
				rf.mu.Unlock()
				return
			}
		} else {
			// log inconsistency
			rf.nextIndex[i] = nextIndex - 1
			// retry
			go rf.issueOneHeartbeat(i, isHeartbeat) // <----todo: can remove isHeartBeat
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
	} else {
		if !isHeartbeat {
			rf.mu.Lock()
			// todo: succeed means that it finds the last matched and attach all entries to it
			// we can't just say nextIdx = len(rf.log), bcuz leader may have append a new log entry
			rf.nextIndex[i] = nextIndex + len(arg.Entries)
			rf.matchIndex[i] = rf.nextIndex[i] - 1
			log.Printf("[IssueAE] AE to %d succeeds,update nextIdx: %d, matchIdx: %d", i, rf.nextIndex[i], rf.matchIndex[i])
			rf.mu.Unlock()
			// todo: log successfully AE
		} else {
			// todo: log successfully HB
		}
	}
}

func (rf *Raft) issueHeartbeat() { // todo: rename
	rf.mu.Lock()
	rf.leaderId = rf.me
	rf.state = Leader
	// we don't want to read req of previous leader round // todo: does monitorHB needs this?
	rf.leaderCh = make(chan ChanArg)
	rf.nextIndex = make([]int, len(rf.peers)) // todo: need lock?
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.log)
	}
	rf.mu.Unlock()
	//time.Sleep(150 * time.Millisecond)

	quit := make(chan ChanArg, 1)
	go rf.startTimer(quit, rf.leaderCh, true)
	//rf.mu.Lock()
	//for rf.state == Leader && !rf.killed() {
	for !rf.killed() {
		var isHeartbeat bool
		arg := <-rf.leaderCh

		if arg == RestartTimer {
			quit <- QuitTimer
			isHeartbeat = false
			log.Printf("[IssueHB] leader (id: %d) issuing AppendEntry\n", rf.me)
		} else if arg == Timeout {
			isHeartbeat = true
			log.Printf("[IssueHB] leader (id: %d) issuing Heatbeat\n", rf.me)
		} else if arg == ChangeToFollower {
			return
		}

		for i := range rf.peers {
			go rf.issueOneHeartbeat(i, isHeartbeat) // todo: rename // lock here?
			//go rf.issueOneHeartbeat(i) // todo: rename
		}
		quit = make(chan ChanArg, 1)
		go rf.startTimer(quit, rf.leaderCh, true)
		// update commitIdx // todo: check (thread?) // todo: uncomment

		rf.mu.Lock()
		fmt.Println("Updating commitIdx")
		stat := make([]int, len(rf.log))
		for i, s := range rf.matchIndex {
			fmt.Printf("ocunt stat, server: %d, matchOdx: %d\n", i, s)
			stat[s]++
		}

		majority := len(rf.peers)/2 + 1
		count := 0
		// reverse loop to get biggest index that has majority
		for i := len(rf.log) - 1; i >= 0; i-- {
			count += stat[i]
			if count >= majority && i > rf.commitIdx && rf.log[i].Term == rf.currentTerm {
				rf.commitIdx = i
				break
			}
		}
		log.Printf("[Commit] Leader (id: %d) update commitIdx as %d\n", rf.me, rf.commitIdx)
		// todo: this is entirely same as AE, but leader doesn't go thru AE so here is it!
		if rf.commitIdx > rf.lastApplied {
			start := rf.lastApplied + 1
			end := rf.commitIdx + 1
			for i := start; i < end; i++ {
				logEntry := rf.log[i]
				rf.applyCh <- ApplyMsg{Command: logEntry.Command, CommandIndex: i, CommandValid: true}
				rf.lastApplied++
			}
			log.Printf("[AE] server (id: %d) applies index: %d~%d, updates lastApplied to %d\n", rf.me, start, end-1, rf.lastApplied)
		}
		rf.mu.Unlock()

	}
	//rf.mu.Unlock()
}
