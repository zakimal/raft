package raft

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const isDebug = 1

type LogEntry struct {
	Command interface{}
	Term    int
}

type State int

const (
	Follower State = iota
	Candidate
	Leader
	Dead
)

type ConsensusModule struct {
	mu sync.Mutex

	serverID int
	peerIDs  []int
	server   *Server

	currentTerm int
	votedFor    int
	logs        []LogEntry

	commitIndex     int
	lastApplied     int
	state           State
	electionResetAt time.Time

	nextIndex  map[int]int
	matchIndex map[int]int
}

func NewConsensusModule(serverID int, peerIDs []int, server *Server, ready <-chan interface{}) *ConsensusModule {
	cm := new(ConsensusModule)
	cm.serverID = serverID
	cm.peerIDs = peerIDs
	cm.server = server
	cm.state = Follower
	cm.votedFor = -1

	go func() {
		<-ready
		cm.mu.Lock()
		cm.electionResetAt = time.Now()
		cm.mu.Unlock()
		cm.startElectionTimer()
	}()

	return cm
}

func (cm *ConsensusModule) Report() (serverID int, term int, isLeader bool) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.serverID, cm.currentTerm, cm.state == Leader
}

func (cm *ConsensusModule) Stop() {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.state = Dead
	cm.dlog("becomes DEAD")
}

func (cm *ConsensusModule) dlog(format string, args ...interface{}) {
	if 0 < isDebug {
		format = fmt.Sprintf("[%d] ", cm.serverID) + format
		log.Printf(format, args...)
	}
}

type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term           int
	HasVoteGranted bool
}

func (cm *ConsensusModule) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.state == Dead {
		return nil
	}
	cm.dlog("RequestVote: %+v [currentTerm=%d, votedFor=%d]", args, cm.currentTerm, cm.votedFor)

	if cm.currentTerm < args.Term {
		cm.dlog("... term out of date in RequestVote")
		cm.becomeFollower(args.Term)
	}

	if cm.currentTerm == args.Term && (cm.votedFor == -1 || cm.votedFor == args.CandidateID) {
		reply.HasVoteGranted = true
		cm.votedFor = args.CandidateID
		cm.electionResetAt = time.Now()
	} else {
		reply.HasVoteGranted = false
	}

	reply.Term = cm.currentTerm
	cm.dlog("... RequestVote reply: %+v", reply)
	return nil
}

type AppendEntriesArgs struct {
	Term     int
	LeaderID int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term         int
	HasSucceeded bool
}

func (cm *ConsensusModule) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.state == Dead {
		return nil
	}

	cm.dlog("AppendEntries: %+v", args)

	if cm.currentTerm < args.Term {
		cm.dlog("... term out of date in AppendEntries")
		cm.becomeFollower(args.Term)
	}

	reply.HasSucceeded = false
	if args.Term == cm.currentTerm {
		if cm.state != Follower {
			cm.becomeFollower(args.Term)
		}
		cm.electionResetAt = time.Now()
		reply.HasSucceeded = true
	}

	reply.Term = cm.currentTerm
	cm.dlog("AppendEntries: %+v", *reply)
	return nil
}

func (cm *ConsensusModule) electionTimeout() time.Duration {
	if 0 < len(os.Getenv("RAFT_FORCE_MORE_REELECTION")) && rand.Intn(3) == 0 {
		return time.Duration(150) * time.Millisecond
	} else {
		return time.Duration(150 + rand.Intn(150)) * time.Millisecond
	}
}

func (cm *ConsensusModule) startElectionTimer() {
	timeoutDuration := cm.electionTimeout()
	cm.mu.Lock()
	termStarted := cm.currentTerm
	cm.dlog("election timer started (%v), term=%d", timeoutDuration, termStarted)

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		<-ticker.C

		cm.mu.Lock()
		if cm.state != Candidate && cm.state != Follower {
			cm.dlog("in election timer state=%s, bailing out", cm.state)
			cm.mu.Unlock()
			return
		}

		if termStarted != cm.currentTerm {
			cm.dlog("in election timer term changed from %d to $d, bailing out", termStarted, cm.currentTerm)
			cm.mu.Unlock()
			return
		}

		if elaped := time.Since(cm.electionResetAt); timeoutDuration <= elaped {
			cm.startElection()
			cm.mu.Unlock()
			return
		}
		cm.mu.Unlock()
	}
}

func (cm *ConsensusModule) startElection() {
	cm.state = Candidate
	cm.currentTerm += 1
	savedCurrentTerm := cm.currentTerm
	cm.votedFor = cm.serverID // vote for self
	cm.dlog("becomes Candidate (currentTerm=%d); logs=%v", savedCurrentTerm, cm.logs)

	var votesReceived int32 = 1

	for _, peerID := range cm.peerIDs {
		go func(peerID int) {
			args := RequestVoteArgs{
				Term:         savedCurrentTerm,
				CandidateID:  cm.serverID,
			}
			var reply RequestVoteReply

			cm.dlog("sending RequestVote to peer %d: %+v", peerID, args)
			if err := cm.server.Call(peerID, "ConsensusModule.RequestVote", args, &reply); err == nil {
				cm.mu.Lock()
				defer cm.mu.Unlock()
				cm.dlog("received RequestVoteReply: %+v", reply)

				if cm.state != Candidate {
					cm.dlog("while waiting for reply, state has changed")
					return
				}

				if savedCurrentTerm < reply.Term {
					cm.dlog("term out of date in RequestVoteReply")
					cm.becomeFollower(reply.Term)
					return
				} else if reply.Term == savedCurrentTerm {
					if reply.HasVoteGranted {
						votes := int(atomic.AddInt32(&votesReceived, 1))
						if len(cm.peerIDs) + 1 < votes*2 {
							cm.dlog("wins election with %d votes, so I am leader", votes)
							cm.startLeader()
							return
						}
					}
				}
			}
		}(peerID)
	}

	go cm.startElectionTimer()
}

func (cm *ConsensusModule) becomeFollower(term int) {
	cm.dlog("becomes Follower with term=%d; log=%v", term, cm.logs)
	cm.state = Follower
	cm.currentTerm = term
	cm.votedFor = -1
	cm.electionResetAt = time.Now()

	go cm.startElectionTimer()
}

func (cm *ConsensusModule) startLeader() {
	cm.state = Leader
	cm.dlog("becomes Leader; term=%d, log=%v", cm.currentTerm, cm.logs)

	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()

		for {
			cm.sendHeartBeats()
			<-ticker.C

			cm.mu.Lock()
			if cm.state != Leader {
				cm.mu.Unlock()
				return
			}
			cm.mu.Unlock()
		}
	}()
}

func (cm *ConsensusModule) sendHeartBeats() {
	cm.mu.Lock()
	savedCurrentTerm := cm.currentTerm
	cm.mu.Unlock()

	for _, peerID := range cm.peerIDs {
		args := AppendEntriesArgs{
			Term:         savedCurrentTerm,
			LeaderID:     cm.serverID,
		}
		go func(peerID int) {
			cm.dlog("send AppendEntries to %v: args=%v", peerID, args)
			var reply AppendEntriesReply
			if err := cm.server.Call(peerID, "ConsensusModule.AppendEntries", args, &reply); err == nil {
				cm.mu.Lock()
				defer cm.mu.Unlock()
				if savedCurrentTerm < reply.Term {
					cm.dlog("term out of date in heartbeat reply")
					cm.becomeFollower(reply.Term)
					return
				}
			}
		}(peerID)
	}
}


