package raft

import (
	"fmt"
	"net"
	"net/rpc"
	"sync"
	"time"
)

// --- RPC argument/reply types ---

// RequestVoteArgs is sent by candidates to request votes.
type RequestVoteArgs struct {
	Term         uint64
	CandidateID  string
	LastLogIndex uint64
	LastLogTerm  uint64
}

// RequestVoteReply is the response to a RequestVote RPC.
type RequestVoteReply struct {
	Term        uint64
	VoteGranted bool
}

// AppendEntriesArgs is sent by the leader for replication and heartbeats.
type AppendEntriesArgs struct {
	Term         uint64
	LeaderID     string
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}

// AppendEntriesReply is the response to an AppendEntries RPC.
type AppendEntriesReply struct {
	Term    uint64
	Success bool
}

// --- RPC service ---

// RPCService exposes Raft RPC methods.
type RPCService struct {
	raft *Raft
}

// RequestVote handles the RequestVote RPC.
func (s *RPCService) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	s.raft.handleRequestVote(args, reply)
	return nil
}

// AppendEntries handles the AppendEntries RPC.
func (s *RPCService) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	s.raft.handleAppendEntries(args, reply)
	return nil
}

// --- Transport ---

// Transport handles RPC communication between Raft nodes.
type Transport struct {
	mu       sync.Mutex
	listener net.Listener
	addr     string
	peers    map[string]*rpc.Client
	server   *rpc.Server
	closeCh  chan struct{}
	wg       sync.WaitGroup
}

// NewTransport creates a new Transport that listens on the given address.
func NewTransport(addr string) (*Transport, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("transport: listen: %w", err)
	}
	return &Transport{
		listener: ln,
		addr:     ln.Addr().String(),
		peers:    make(map[string]*rpc.Client),
		server:   rpc.NewServer(),
		closeCh:  make(chan struct{}),
	}, nil
}

// Start registers the RPC service and begins accepting connections.
func (t *Transport) Start(svc *RPCService) error {
	if err := t.server.RegisterName("RPCService", svc); err != nil {
		return fmt.Errorf("transport: register: %w", err)
	}
	t.wg.Add(1)
	go t.acceptLoop()
	return nil
}

func (t *Transport) acceptLoop() {
	defer t.wg.Done()
	for {
		conn, err := t.listener.Accept()
		if err != nil {
			select {
			case <-t.closeCh:
				return
			default:
				continue
			}
		}
		go t.server.ServeConn(conn)
	}
}

// getClient returns a cached or new RPC client for the peer.
func (t *Transport) getClient(peer string) (*rpc.Client, error) {
	t.mu.Lock()
	client, ok := t.peers[peer]
	t.mu.Unlock()

	if ok {
		return client, nil
	}

	conn, err := net.DialTimeout("tcp", peer, 2*time.Second)
	if err != nil {
		return nil, err
	}
	client = rpc.NewClient(conn)

	t.mu.Lock()
	// Check again in case another goroutine connected.
	if existing, ok := t.peers[peer]; ok {
		t.mu.Unlock()
		client.Close()
		return existing, nil
	}
	t.peers[peer] = client
	t.mu.Unlock()
	return client, nil
}

// removeClient removes a cached client (on error).
func (t *Transport) removeClient(peer string) {
	t.mu.Lock()
	if client, ok := t.peers[peer]; ok {
		client.Close()
		delete(t.peers, peer)
	}
	t.mu.Unlock()
}

// SendRequestVote sends a RequestVote RPC to the given peer.
func (t *Transport) SendRequestVote(peer string, args *RequestVoteArgs) (*RequestVoteReply, error) {
	client, err := t.getClient(peer)
	if err != nil {
		return nil, err
	}
	reply := &RequestVoteReply{}
	err = client.Call("RPCService.RequestVote", args, reply)
	if err != nil {
		t.removeClient(peer)
		return nil, err
	}
	return reply, nil
}

// SendAppendEntries sends an AppendEntries RPC to the given peer.
func (t *Transport) SendAppendEntries(peer string, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	client, err := t.getClient(peer)
	if err != nil {
		return nil, err
	}
	reply := &AppendEntriesReply{}
	err = client.Call("RPCService.AppendEntries", args, reply)
	if err != nil {
		t.removeClient(peer)
		return nil, err
	}
	return reply, nil
}

// Addr returns the address the transport is listening on.
func (t *Transport) Addr() string {
	return t.addr
}

// Close shuts down the transport.
func (t *Transport) Close() error {
	close(t.closeCh)
	t.listener.Close()
	t.wg.Wait()

	t.mu.Lock()
	for _, client := range t.peers {
		client.Close()
	}
	t.peers = make(map[string]*rpc.Client)
	t.mu.Unlock()
	return nil
}
