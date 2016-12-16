package raft


import (
	"sync"
	"labrpc"
	"bytes"
	"encoding/gob"
	"time"
	"math/rand"
)
const (
	LEADER = iota
	CANDIDATE
	FOLLOWER
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type LogEntry struct {
	Term int
	LogComd interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	Mu        sync.Mutex
	Peers     []*labrpc.ClientEnd
	persister *Persister
	Me        int // index into Peers[]

	//important numbers
	Status int
	VoteCount int

	//channels for events
	newCommit chan bool
	RecievedBeat chan bool
	GaveVote chan bool
	BecameLeader chan bool

	//persistent state on all server
	CurrentTerm int
	VotedFor int
	Log	[]LogEntry

	//volatile state on all servers
	CommitIndex int
	LastApplied int

	//volatile state on leader
	NextIndex []int
	MatchIndex []int
}

//-----------------------------------------------------------------
//----------------------RAFT METHODS  -----------------------------
//-----------------------------------------------------------------


// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	rf.Mu.Lock()
	term := rf.CurrentTerm
	isLeader := rf.Status == LEADER
	if isLeader {
		index = rf.LastIndex() + 1
		rf.Log = append(rf.Log, LogEntry{Term:term,LogComd:command}) 
		rf.persist()
	}
	rf.Mu.Unlock()
	return index, term, isLeader
}

// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

// return CurrentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool

	rf.Mu.Lock()
	if rf.Status == LEADER {
		isleader = true
	} else {
		isleader = false
	}
	term = rf.CurrentTerm
	rf.Mu.Unlock()
	return term, isleader
}

//some helper methods
func (rf *Raft) LastIndex() int {
	return len(rf.Log) - 1
}
func (rf *Raft) LastTerm() int {
	return rf.Log[rf.LastIndex()].Term
}


//-----------------------------------------------------------------
//----------------------PERSISTENCE CODE --------------------------
//-----------------------------------------------------------------


// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
 	e := gob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.CurrentTerm)
	d.Decode(&rf.VotedFor)
	d.Decode(&rf.Log)
}

//-----------------------------------------------------------------
//---------------------------VOTING CODE --------------------------
//-----------------------------------------------------------------


// example RequestVote RPC arguments structure.
type RequestVoteArgs struct {
	Term int
	CandidateId int
	LastLogTerm int
	LastLogIndex int
}


type RequestVoteReply struct {
	Term int
	VoteGranted bool
}


// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.Mu.Lock()
	defer rf.Mu.Unlock()
	defer rf.persist()

	//rule for all RPC Requests: if request contains term T > currentTerm,
	//set currentTerm to T, convert to follower
	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.Status = FOLLOWER
		rf.VotedFor = -1
	}

	//reply false if term < currentTerm
	if args.Term < rf.CurrentTerm {
		reply.VoteGranted = false
		reply.Term = rf.CurrentTerm
		return
	}

	votedForCondition := rf.VotedFor == -1 || rf.VotedFor == args.CandidateId
	upToDateCondition := false
	if (args.LastLogTerm == rf.LastTerm()){
		upToDateCondition = args.LastLogIndex >= rf.LastIndex()
	} else {
		upToDateCondition = args.LastLogTerm > rf.LastTerm()
	}

	if (votedForCondition && upToDateCondition) {
		rf.GaveVote <- true
		rf.Status = FOLLOWER
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = true
		rf.VotedFor = args.CandidateId
	}
}

func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.Peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//-----------------------------------------------------------------
//------------------------APPENDENTRIES CODE-----------------------
//-----------------------------------------------------------------

type AppendEntriesArgs struct {
	// Your data here.
	Term int
	LeaderId int
	PrevLogTerm int
	PrevLogIndex int
	Entries []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	// Your data here.
	Term int
	Success bool
	NextIndex int
}


func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here.
	rf.Mu.Lock()
	defer rf.Mu.Unlock()
	defer rf.persist()

	//if leader term less than our rf term, reply false
	reply.Success = false
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.NextIndex = rf.LastIndex() + 1
		return
	}

	//we now can safely assume that the appendEntries sender is the true leader,
	//so give it a heartbeat
	rf.RecievedBeat <- true
	rf.VotedFor = -1

	//rule for all servers: if RPC requester's term is higher than RPC reciever's, 
	//adjust RPC reciever's term and make it a follower.
	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.Status = FOLLOWER
	}

	reply.Term = args.Term

	//if rf log doesn't even have prevLogIndex, ie is too short,
	//reply false and also send un updated NextIndex value
	if args.PrevLogIndex > rf.LastIndex() {
		reply.NextIndex = rf.LastIndex() + 1
		return
	}

	//reply false if rf log doesn't contain an entry at prevLogIndex
	//whose term matches prevLogTerm. We also send the first index
	//of the erroneous term as the nextIndex
	ourTerm := rf.Log[args.PrevLogIndex].Term
	if args.PrevLogTerm != ourTerm {
		for i := args.PrevLogIndex - 1 ; i >= 0; i-- {
			if rf.Log[i].Term != ourTerm {
				reply.NextIndex = i + 1
				break
			}
		}
		return
	}

	rf.Log = rf.Log[: args.PrevLogIndex+1]     //cut down our log
	rf.Log = append(rf.Log, args.Entries...)   //append new entries from leader log to our log

	//update the commit index if need be
	if args.LeaderCommit > rf.CommitIndex {
		rf.CommitIndex = Min(args.LeaderCommit, rf.LastIndex())
		rf.newCommit <- true
	}
	reply.Success = true
	reply.NextIndex = rf.LastIndex() + 1
	return
}

func Min(x, y int) int {
    if x < y {
        return x
    }
    return y
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.Peers[server].Call("Raft.AppendEntries", args, reply)
	rf.Mu.Lock()
	defer rf.Mu.Unlock()
	if ok {
		if (rf.Status != LEADER || args.Term != rf.CurrentTerm) {
			return ok
		}

		if reply.Term > rf.CurrentTerm {
			rf.CurrentTerm = reply.Term
			rf.Status = FOLLOWER
			rf.VotedFor = -1
			rf.persist()
			return ok
		}
		if reply.Success {
			if len(args.Entries) > 0 {
				rf.NextIndex[server] = reply.NextIndex
				rf.MatchIndex[server] = rf.NextIndex[server] - 1
			}
		} else {
			rf.NextIndex[server] = reply.NextIndex
		}
	}
	return ok
}

//-----------------------------------------------------------------
//------------------------SEND TO ALL SERVERS CODE-----------------
//-----------------------------------------------------------------

func (rf *Raft) requestVoteFromAll() {
	rf.Mu.Lock()
	args := RequestVoteArgs{Term: rf.CurrentTerm, CandidateId: rf.Me, LastLogTerm: rf.LastTerm(), LastLogIndex: rf.LastIndex()}
	rf.Mu.Unlock()

	for i := range rf.Peers {
		if i != rf.Me && rf.Status == CANDIDATE {
			go func(i int){

				//send request vote RPC
				var reply RequestVoteReply
				rf.sendRequestVote(i, args, &reply)

				//process the recieved reply
				rf.Mu.Lock()
				defer rf.Mu.Unlock()
				
				if reply.Term > rf.CurrentTerm {
					rf.CurrentTerm = reply.Term
					rf.Status = FOLLOWER
					rf.VotedFor = -1
					rf.persist()
				}

				if reply.VoteGranted {
					rf.VoteCount++
					if rf.Status == CANDIDATE && rf.VoteCount > len(rf.Peers)/2 {
						rf.Status = FOLLOWER
						rf.BecameLeader <- true
					}
				}
			}(i)
		}
	}
}

func (rf *Raft) sendAppendEntriesToAll() {
	rf.Mu.Lock()
	defer rf.Mu.Unlock()

	//send AppendEntries to all peers
	for i := range rf.Peers {
		if i != rf.Me && rf.Status == LEADER {

			//populate AppendEntries argument
			startIndex := rf.NextIndex[i]
			pLogIndex := startIndex - 1
			pLogTerm := rf.Log[pLogIndex].Term
			entries := make([]LogEntry, len(rf.Log[startIndex:]))
			copy(entries, rf.Log[startIndex:])
			args := AppendEntriesArgs{Term: rf.CurrentTerm, LeaderId: rf.Me, PrevLogIndex: pLogIndex, PrevLogTerm: pLogTerm, Entries: entries, LeaderCommit: rf.CommitIndex}

			//send AppendEntriesRPC
			go func(i int, args AppendEntriesArgs) {
				reply := AppendEntriesReply{}
				rf.sendAppendEntries(i, args, &reply)
			}(i,args)
		}
	}

	//If there exists an N such that N > commitIndex, a majority
	//of matchIndex[i] >= N, and log[N].term == currentTerm, set CommitIndex = N
	N := rf.CommitIndex
	updated := false
	for n:= rf.CommitIndex + 1; n <= rf.LastIndex(); n++ {
		ct:= 1
		for i:= range rf.Peers {
			if (i == rf.Me) {
				continue
			}

			if (rf.MatchIndex[i] >= n){
				ct++
			}
		}

		if (2*ct > len(rf.Peers) && rf.Log[n].Term == rf.CurrentTerm) {
			N = n
			updated = true
		}
	}
	if updated {
		rf.CommitIndex = N
		rf.newCommit <- true		
	}

}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in Peers[]. this
// server's port is Peers[me]. all the servers' Peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() Must return quickly, so it should start goroutines
// for any long-running work.
func Make(Peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.Peers = Peers
	rf.persister = persister
	rf.Me = me

	// Your initialization code here.
	rf.Status = FOLLOWER
	rf.VotedFor = -1
	rf.Log = append(rf.Log, LogEntry{Term: 0})
	rf.CurrentTerm = 0
	rf.newCommit = make(chan bool,100)
	rf.RecievedBeat = make(chan bool,100)
	rf.GaveVote = make(chan bool,100)
	rf.BecameLeader = make(chan bool,100)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go func() {
		for {
			select {
			case <-rf.newCommit:
				rf.Mu.Lock()
			    CommitIndex := rf.CommitIndex
				for i := rf.LastApplied+1; i <= CommitIndex; i++ {
					msg := ApplyMsg{Index: i, Command: rf.Log[i].LogComd}
					applyCh <- msg
					rf.LastApplied = i
				}
				rf.Mu.Unlock()
			}
		}
	}()

	go func() {
		for {
			//------------------//
			//FOLLOWER STATE
			//------------------//
			if (rf.Status == FOLLOWER) {
				select {
				case <-rf.RecievedBeat:
				case <-rf.GaveVote:
				case <-time.After(time.Duration(150+rand.Int31n(350)) * time.Millisecond):
					rf.Status = CANDIDATE
				}
			//------------------/
			//CANDIDATE STATE
			//------------------/
			} else if (rf.Status == CANDIDATE) {
				rf.Mu.Lock()
				rf.VotedFor = rf.Me
				rf.VoteCount = 1
				rf.CurrentTerm++
				rf.persist()
				rf.Mu.Unlock()
			 go rf.requestVoteFromAll()
				select {
				case <-time.After(time.Duration(150+rand.Int31n(350)) * time.Millisecond):
				case <-rf.RecievedBeat:
					rf.Mu.Lock()
					rf.Status = FOLLOWER
					rf.Mu.Unlock()
				case <-rf.BecameLeader:
					rf.Mu.Lock()
					rf.NextIndex = make([]int,len(rf.Peers))
					rf.MatchIndex = make([]int,len(rf.Peers))
					for i := range rf.Peers {
						rf.NextIndex[i] = rf.LastIndex() + 1
						rf.MatchIndex[i] = 0
					}
					rf.Status = LEADER
					rf.Mu.Unlock()
				}
			//------------------/
			//LEADER STATE
			//------------------/
			} else if (rf.Status == LEADER) {
				rf.sendAppendEntriesToAll()
				time.Sleep(50 * time.Millisecond)
			}
		}
	}()

	return rf
}
