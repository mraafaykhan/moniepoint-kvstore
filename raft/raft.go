package raft

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// State represents the role of a Raft node.
type State int

const (
	Follower  State = iota
	Candidate
	Leader
)

func (s State) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

var (
	ErrNotLeader     = errors.New("raft: not the leader")
	ErrProposalTimeout = errors.New("raft: proposal timeout")
	ErrStopped       = errors.New("raft: node stopped")
)

// Config holds timing configuration for a Raft instance.
type Config struct {
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	HeartbeatInterval  time.Duration
	ProposalTimeout    time.Duration
}

// DefaultConfig returns the default Raft timing configuration.
func DefaultConfig() Config {
	return Config{
		ElectionTimeoutMin: 300 * time.Millisecond,
		ElectionTimeoutMax: 500 * time.Millisecond,
		HeartbeatInterval:  100 * time.Millisecond,
		ProposalTimeout:    5 * time.Second,
	}
}

// proposal tracks a pending client proposal waiting for commit.
type proposal struct {
	index uint64
	doneCh chan error
}

// Raft implements the Raft consensus protocol.
type Raft struct {
	mu sync.Mutex

	// Persistent state
	currentTerm uint64
	votedFor    string
	log         *Log

	// Volatile state
	state       State
	commitIndex uint64
	lastApplied uint64
	leaderID    string

	// Leader state
	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	// Config
	nodeID string
	peers  []string
	config Config

	// Channels
	applyCh  chan LogEntry
	commitCh chan struct{}

	// Pending proposals
	proposals []*proposal

	// Timers
	electionTimer *time.Timer
	heartbeatTick *time.Ticker

	// Transport
	transport *Transport

	// Persistent state file
	stateFile string
	dir       string

	// Lifecycle
	closeCh chan struct{}
	wg      sync.WaitGroup
}

// NewRaft creates a new Raft instance. It loads persistent state and opens the log.
func NewRaft(nodeID string, peers []string, dir string, raftAddr string) (*Raft, error) {
	return NewRaftWithConfig(nodeID, peers, dir, raftAddr, DefaultConfig())
}

// NewRaftWithConfig creates a new Raft instance with custom timing configuration.
func NewRaftWithConfig(nodeID string, peers []string, dir string, raftAddr string, config Config) (*Raft, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	raftLog, err := OpenLog(dir)
	if err != nil {
		return nil, fmt.Errorf("raft: open log: %w", err)
	}

	transport, err := NewTransport(raftAddr)
	if err != nil {
		raftLog.Close()
		return nil, fmt.Errorf("raft: create transport: %w", err)
	}

	r := &Raft{
		log:       raftLog,
		state:     Follower,
		nodeID:    nodeID,
		peers:     peers,
		config:    config,
		applyCh:   make(chan LogEntry, 256),
		commitCh:  make(chan struct{}, 1),
		transport: transport,
		stateFile: filepath.Join(dir, "raft-state.bin"),
		dir:       dir,
		closeCh:   make(chan struct{}),
	}

	if err := r.loadState(); err != nil {
		raftLog.Close()
		transport.Close()
		return nil, fmt.Errorf("raft: load state: %w", err)
	}

	return r, nil
}

// Start begins the Raft protocol: starts transport, main loop, and apply loop.
func (r *Raft) Start() error {
	svc := &RPCService{raft: r}
	if err := r.transport.Start(svc); err != nil {
		return err
	}

	r.mu.Lock()
	r.electionTimer = time.NewTimer(r.randomElectionTimeout())
	r.heartbeatTick = time.NewTicker(r.config.HeartbeatInterval)
	r.mu.Unlock()

	r.wg.Add(2)
	go r.mainLoop()
	go r.applyLoop()

	return nil
}

// Stop signals the Raft instance to shut down and waits for goroutines to finish.
func (r *Raft) Stop() error {
	close(r.closeCh)
	r.wg.Wait()

	r.mu.Lock()
	if r.electionTimer != nil {
		r.electionTimer.Stop()
	}
	if r.heartbeatTick != nil {
		r.heartbeatTick.Stop()
	}
	// Fail any pending proposals.
	for _, p := range r.proposals {
		select {
		case p.doneCh <- ErrStopped:
		default:
		}
	}
	r.proposals = nil
	r.mu.Unlock()

	r.transport.Close()
	r.log.Close()
	return nil
}

// Propose submits a command to the Raft cluster. Blocks until committed or timeout.
// Returns ErrNotLeader if this node is not the leader.
func (r *Raft) Propose(cmdType CommandType, data []byte) error {
	r.mu.Lock()
	if r.state != Leader {
		r.mu.Unlock()
		return ErrNotLeader
	}

	entry := LogEntry{
		Term:    r.currentTerm,
		Index:   r.log.LastIndex() + 1,
		CmdType: cmdType,
		Data:    data,
	}

	if err := r.log.Append(entry); err != nil {
		r.mu.Unlock()
		return err
	}

	// Track proposal.
	p := &proposal{
		index:  entry.Index,
		doneCh: make(chan error, 1),
	}
	r.proposals = append(r.proposals, p)

	// Update own matchIndex.
	r.matchIndex[r.transport.Addr()] = entry.Index

	r.mu.Unlock()

	// Trigger immediate replication.
	r.sendHeartbeats()

	// Wait for commit or timeout.
	select {
	case err := <-p.doneCh:
		return err
	case <-time.After(r.config.ProposalTimeout):
		return ErrProposalTimeout
	case <-r.closeCh:
		return ErrStopped
	}
}

// IsLeader returns true if this node is the current leader.
func (r *Raft) IsLeader() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.state == Leader
}

// LeaderAddr returns the address of the current leader.
func (r *Raft) LeaderAddr() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.leaderID
}

// NodeID returns this node's ID.
func (r *Raft) NodeID() string {
	return r.nodeID
}

// ApplyCh returns the channel on which committed entries are delivered.
func (r *Raft) ApplyCh() <-chan LogEntry {
	return r.applyCh
}

// TransportAddr returns the address the transport is listening on.
func (r *Raft) TransportAddr() string {
	return r.transport.Addr()
}

// RaftStatus holds a snapshot of Raft state for external inspection.
type RaftStatus struct {
	NodeID      string
	State       string // "Leader", "Follower", "Candidate"
	Term        uint64
	CommitIndex uint64
	LastApplied uint64
	LogLength   uint64
	LeaderID    string
	Peers       []string
}

// Status returns a point-in-time snapshot of the Raft state.
func (r *Raft) Status() RaftStatus {
	r.mu.Lock()
	defer r.mu.Unlock()
	return RaftStatus{
		NodeID:      r.nodeID,
		State:       r.state.String(),
		Term:        r.currentTerm,
		CommitIndex: r.commitIndex,
		LastApplied: r.lastApplied,
		LogLength:   r.log.LastIndex(),
		LeaderID:    r.leaderID,
		Peers:       r.peers,
	}
}

// --- Main loop ---

func (r *Raft) mainLoop() {
	defer r.wg.Done()
	for {
		r.mu.Lock()
		elTimer := r.electionTimer
		hbTick := r.heartbeatTick
		r.mu.Unlock()

		select {
		case <-r.closeCh:
			return
		case <-elTimer.C:
			r.startElection()
		case <-hbTick.C:
			r.mu.Lock()
			isLeader := r.state == Leader
			r.mu.Unlock()
			if isLeader {
				r.sendHeartbeats()
			}
		}
	}
}

// --- Elections ---

func (r *Raft) startElection() {
	r.mu.Lock()
	r.state = Candidate
	r.currentTerm++
	r.votedFor = r.nodeID
	r.leaderID = ""
	currentTerm := r.currentTerm
	lastLogIndex := r.log.LastIndex()
	lastLogTerm := r.log.LastTerm()
	r.persistState()
	r.resetElectionTimerLocked()
	peers := make([]string, len(r.peers))
	copy(peers, r.peers)
	r.mu.Unlock()

	votes := 1 // vote for self
	total := len(peers) + 1
	majority := total/2 + 1

	var voteMu sync.Mutex
	var wg sync.WaitGroup

	for _, peer := range peers {
		wg.Add(1)
		go func(p string) {
			defer wg.Done()
			reply, err := r.transport.SendRequestVote(p, &RequestVoteArgs{
				Term:         currentTerm,
				CandidateID:  r.nodeID,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			})
			if err != nil {
				return
			}

			r.mu.Lock()
			if reply.Term > r.currentTerm {
				r.becomeFollowerLocked(reply.Term, "")
				r.mu.Unlock()
				return
			}
			r.mu.Unlock()

			if reply.VoteGranted {
				voteMu.Lock()
				votes++
				won := votes >= majority
				voteMu.Unlock()
				if won {
					r.mu.Lock()
					if r.state == Candidate && r.currentTerm == currentTerm {
						r.becomeLeaderLocked()
					}
					r.mu.Unlock()
				}
			}
		}(peer)
	}

	// Don't block waiting for all replies - they resolve asynchronously.
	go func() { wg.Wait() }()
}

func (r *Raft) becomeLeaderLocked() {
	r.state = Leader
	r.leaderID = r.transport.Addr()
	r.nextIndex = make(map[string]uint64)
	r.matchIndex = make(map[string]uint64)
	lastIdx := r.log.LastIndex()
	for _, peer := range r.peers {
		r.nextIndex[peer] = lastIdx + 1
		r.matchIndex[peer] = 0
	}
	// Leader's own matchIndex.
	r.matchIndex[r.transport.Addr()] = lastIdx

	// Stop election timer from firing.
	r.electionTimer.Reset(r.randomElectionTimeout() * 10)

	// Send immediate heartbeats (unlock first).
	go r.sendHeartbeats()
}

func (r *Raft) becomeFollowerLocked(term uint64, leaderID string) {
	r.state = Follower
	r.currentTerm = term
	r.votedFor = ""
	if leaderID != "" {
		r.leaderID = leaderID
	}
	r.persistState()
	r.resetElectionTimerLocked()
}

// --- Heartbeats & Replication ---

func (r *Raft) sendHeartbeats() {
	r.mu.Lock()
	if r.state != Leader {
		r.mu.Unlock()
		return
	}
	peers := make([]string, len(r.peers))
	copy(peers, r.peers)
	r.mu.Unlock()

	var wg sync.WaitGroup
	for _, peer := range peers {
		wg.Add(1)
		go func(p string) {
			defer wg.Done()
			r.replicateTo(p)
		}(peer)
	}
	wg.Wait()
}

func (r *Raft) replicateTo(peer string) {
	r.mu.Lock()
	if r.state != Leader {
		r.mu.Unlock()
		return
	}
	currentTerm := r.currentTerm
	nextIdx := r.nextIndex[peer]
	prevLogIndex := uint64(0)
	prevLogTerm := uint64(0)

	if nextIdx > 1 {
		prevLogIndex = nextIdx - 1
		entry := r.log.GetEntry(prevLogIndex)
		if entry != nil {
			prevLogTerm = entry.Term
		}
	}

	entries := r.log.GetEntriesFrom(nextIdx)
	commitIndex := r.commitIndex
	r.mu.Unlock()

	reply, err := r.transport.SendAppendEntries(peer, &AppendEntriesArgs{
		Term:         currentTerm,
		LeaderID:     r.transport.Addr(),
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: commitIndex,
	})
	if err != nil {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if reply.Term > r.currentTerm {
		r.becomeFollowerLocked(reply.Term, "")
		return
	}

	if r.state != Leader || r.currentTerm != currentTerm {
		return
	}

	if reply.Success {
		if len(entries) > 0 {
			r.nextIndex[peer] = entries[len(entries)-1].Index + 1
			r.matchIndex[peer] = entries[len(entries)-1].Index
		}
		r.advanceCommitIndexLocked()
	} else {
		// Decrement nextIndex and retry.
		if r.nextIndex[peer] > 1 {
			r.nextIndex[peer]--
		}
	}
}

func (r *Raft) advanceCommitIndexLocked() {
	// Find the highest N such that a majority of matchIndex >= N and log[N].term == currentTerm.
	lastIdx := r.log.LastIndex()
	for n := lastIdx; n > r.commitIndex; n-- {
		entry := r.log.GetEntry(n)
		if entry == nil || entry.Term != r.currentTerm {
			continue
		}
		count := 1 // self
		for _, peer := range r.peers {
			if r.matchIndex[peer] >= n {
				count++
			}
		}
		total := len(r.peers) + 1
		if count > total/2 {
			r.commitIndex = n
			// Signal apply loop.
			select {
			case r.commitCh <- struct{}{}:
			default:
			}
			// Resolve pending proposals.
			r.resolveProposalsLocked()
			break
		}
	}
}

func (r *Raft) resolveProposalsLocked() {
	remaining := r.proposals[:0]
	for _, p := range r.proposals {
		if p.index <= r.commitIndex {
			select {
			case p.doneCh <- nil:
			default:
			}
		} else {
			remaining = append(remaining, p)
		}
	}
	r.proposals = remaining
}

// --- Apply loop ---

func (r *Raft) applyLoop() {
	defer r.wg.Done()
	for {
		select {
		case <-r.closeCh:
			return
		case <-r.commitCh:
			r.applyCommitted()
		}
	}
}

func (r *Raft) applyCommitted() {
	r.mu.Lock()
	commitIndex := r.commitIndex
	lastApplied := r.lastApplied
	r.mu.Unlock()

	for idx := lastApplied + 1; idx <= commitIndex; idx++ {
		entry := r.log.GetEntry(idx)
		if entry == nil {
			break
		}
		select {
		case r.applyCh <- *entry:
		case <-r.closeCh:
			return
		}
		r.mu.Lock()
		r.lastApplied = idx
		r.mu.Unlock()
	}
}

// --- RPC handlers ---

func (r *Raft) handleRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	r.mu.Lock()
	defer r.mu.Unlock()

	reply.Term = r.currentTerm
	reply.VoteGranted = false

	if args.Term < r.currentTerm {
		return
	}

	if args.Term > r.currentTerm {
		r.becomeFollowerLocked(args.Term, "")
		reply.Term = r.currentTerm
	}

	// Check if we can vote for this candidate.
	if r.votedFor != "" && r.votedFor != args.CandidateID {
		return
	}

	// Check log up-to-dateness.
	lastTerm := r.log.LastTerm()
	lastIndex := r.log.LastIndex()
	if args.LastLogTerm < lastTerm {
		return
	}
	if args.LastLogTerm == lastTerm && args.LastLogIndex < lastIndex {
		return
	}

	// Grant vote.
	r.votedFor = args.CandidateID
	r.persistState()
	r.resetElectionTimerLocked()
	reply.VoteGranted = true
}

func (r *Raft) handleAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	r.mu.Lock()
	defer r.mu.Unlock()

	reply.Term = r.currentTerm
	reply.Success = false

	if args.Term < r.currentTerm {
		return
	}

	if args.Term > r.currentTerm || r.state != Follower {
		r.becomeFollowerLocked(args.Term, args.LeaderID)
		reply.Term = r.currentTerm
	} else {
		r.leaderID = args.LeaderID
		r.resetElectionTimerLocked()
	}

	// Check prev log entry.
	if args.PrevLogIndex > 0 {
		entry := r.log.GetEntry(args.PrevLogIndex)
		if entry == nil {
			return
		}
		if entry.Term != args.PrevLogTerm {
			// Conflict: truncate from here.
			r.log.TruncateFrom(args.PrevLogIndex)
			return
		}
	}

	// Append new entries (handle conflicts).
	if len(args.Entries) > 0 {
		for i, newEntry := range args.Entries {
			existing := r.log.GetEntry(newEntry.Index)
			if existing != nil && existing.Term != newEntry.Term {
				// Conflict at this index: truncate from here and append rest.
				r.log.TruncateFrom(newEntry.Index)
				r.log.Append(args.Entries[i:]...)
				break
			} else if existing == nil {
				// Append all remaining entries.
				r.log.Append(args.Entries[i:]...)
				break
			}
			// else: entry already exists and matches, skip.
		}
	}

	reply.Success = true

	// Update commit index.
	if args.LeaderCommit > r.commitIndex {
		lastIdx := r.log.LastIndex()
		if args.LeaderCommit < lastIdx {
			r.commitIndex = args.LeaderCommit
		} else {
			r.commitIndex = lastIdx
		}
		select {
		case r.commitCh <- struct{}{}:
		default:
		}
	}
}

// --- Persistence ---

// persistState writes currentTerm and votedFor to the state file.
func (r *Raft) persistState() {
	// Format: [Term:8][VotedForLen:4][VotedFor]
	vf := []byte(r.votedFor)
	buf := make([]byte, 8+4+len(vf))
	binary.LittleEndian.PutUint64(buf[0:8], r.currentTerm)
	binary.LittleEndian.PutUint32(buf[8:12], uint32(len(vf)))
	copy(buf[12:], vf)
	os.WriteFile(r.stateFile, buf, 0o644)
}

// loadState reads persistent state from the state file.
func (r *Raft) loadState() error {
	data, err := os.ReadFile(r.stateFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if len(data) < 12 {
		return nil
	}
	r.currentTerm = binary.LittleEndian.Uint64(data[0:8])
	vfLen := binary.LittleEndian.Uint32(data[8:12])
	if len(data) >= 12+int(vfLen) {
		r.votedFor = string(data[12 : 12+vfLen])
	}
	return nil
}

// --- Timer helpers ---

func (r *Raft) randomElectionTimeout() time.Duration {
	min := r.config.ElectionTimeoutMin
	max := r.config.ElectionTimeoutMax
	delta := max - min
	return min + time.Duration(rand.Int63n(int64(delta)))
}

func (r *Raft) resetElectionTimerLocked() {
	if r.electionTimer != nil {
		r.electionTimer.Reset(r.randomElectionTimeout())
	}
}
