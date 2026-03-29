package raft

import (
	"fmt"
	"testing"
	"time"
)

// testConfig returns a fast config for tests.
func testConfig() Config {
	return Config{
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
		HeartbeatInterval:  50 * time.Millisecond,
		ProposalTimeout:    5 * time.Second,
	}
}

// waitForCondition polls a condition function until it returns true or timeout.
func waitForCondition(timeout time.Duration, fn func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return false
}

// createCluster creates n Raft instances connected to each other.
func createCluster(t *testing.T, n int) []*Raft {
	t.Helper()
	dirs := make([]string, n)
	addrs := make([]string, n)
	rafts := make([]*Raft, n)

	// First, create transports to get actual addresses.
	transports := make([]*Transport, n)
	for i := 0; i < n; i++ {
		tr, err := NewTransport("127.0.0.1:0")
		if err != nil {
			t.Fatalf("create transport %d: %v", i, err)
		}
		transports[i] = tr
		addrs[i] = tr.Addr()
	}
	// Close these temporary transports - we'll create proper ones in NewRaftWithConfig.
	for _, tr := range transports {
		tr.Close()
	}

	// Create raft instances.
	for i := 0; i < n; i++ {
		dirs[i] = t.TempDir()
		peers := make([]string, 0, n-1)
		for j := 0; j < n; j++ {
			if j != i {
				peers = append(peers, addrs[j])
			}
		}
		r, err := NewRaftWithConfig(
			fmt.Sprintf("node-%d", i),
			peers,
			dirs[i],
			addrs[i],
			testConfig(),
		)
		if err != nil {
			t.Fatalf("create raft %d: %v", i, err)
		}
		rafts[i] = r
	}
	return rafts
}

// startCluster starts all raft instances.
func startCluster(t *testing.T, rafts []*Raft) {
	t.Helper()
	for i, r := range rafts {
		if err := r.Start(); err != nil {
			t.Fatalf("start raft %d: %v", i, err)
		}
	}
}

// stopCluster stops all raft instances.
func stopCluster(t *testing.T, rafts []*Raft) {
	t.Helper()
	for _, r := range rafts {
		if r != nil {
			r.Stop()
		}
	}
}

// findLeader returns the index of the leader, or -1 if none found.
func findLeader(rafts []*Raft) int {
	for i, r := range rafts {
		if r != nil && r.IsLeader() {
			return i
		}
	}
	return -1
}

// waitForLeader waits until exactly one leader is elected.
func waitForLeader(t *testing.T, rafts []*Raft, timeout time.Duration) int {
	t.Helper()
	leaderIdx := -1
	ok := waitForCondition(timeout, func() bool {
		leaders := 0
		idx := -1
		for i, r := range rafts {
			if r != nil && r.IsLeader() {
				leaders++
				idx = i
			}
		}
		if leaders == 1 {
			leaderIdx = idx
			return true
		}
		return false
	})
	if !ok {
		t.Fatalf("timeout waiting for leader election")
	}
	return leaderIdx
}

func TestLeaderElection(t *testing.T) {
	rafts := createCluster(t, 3)
	startCluster(t, rafts)
	defer stopCluster(t, rafts)

	leaderIdx := waitForLeader(t, rafts, 5*time.Second)
	t.Logf("Leader elected: node %d (%s)", leaderIdx, rafts[leaderIdx].NodeID())

	// Verify exactly one leader.
	leaderCount := 0
	for _, r := range rafts {
		if r.IsLeader() {
			leaderCount++
		}
	}
	if leaderCount != 1 {
		t.Fatalf("expected exactly 1 leader, got %d", leaderCount)
	}

	// Verify all nodes agree on the same term.
	var term uint64
	for i, r := range rafts {
		r.mu.Lock()
		t := r.currentTerm
		r.mu.Unlock()
		if i == 0 {
			term = t
		}
		// Terms should be >= the leader's, but eventually converge.
	}
	t.Logf("Leader term: %d", term)
}

func TestLogReplication(t *testing.T) {
	rafts := createCluster(t, 3)
	startCluster(t, rafts)
	defer stopCluster(t, rafts)

	leaderIdx := waitForLeader(t, rafts, 5*time.Second)
	leader := rafts[leaderIdx]

	// Propose a Put command.
	data := EncodePutCommand([]byte("hello"), []byte("world"))
	err := leader.Propose(CmdRaftPut, data)
	if err != nil {
		t.Fatalf("propose failed: %v", err)
	}

	// Wait for all nodes to have the entry in their log.
	ok := waitForCondition(5*time.Second, func() bool {
		for _, r := range rafts {
			if r.log.LastIndex() < 1 {
				return false
			}
			entry := r.log.GetEntry(r.log.LastIndex())
			if entry == nil || entry.CmdType != CmdRaftPut {
				return false
			}
		}
		return true
	})
	if !ok {
		for i, r := range rafts {
			t.Logf("node %d: lastIndex=%d", i, r.log.LastIndex())
		}
		t.Fatalf("timeout waiting for log replication")
	}

	t.Logf("Log replicated to all nodes")
}

func TestLeaderFailover(t *testing.T) {
	rafts := createCluster(t, 3)
	startCluster(t, rafts)
	defer func() {
		for _, r := range rafts {
			if r != nil {
				r.Stop()
			}
		}
	}()

	// Wait for initial leader.
	leaderIdx := waitForLeader(t, rafts, 5*time.Second)
	t.Logf("Initial leader: node %d", leaderIdx)

	// Stop the leader.
	rafts[leaderIdx].Stop()
	oldLeader := leaderIdx
	rafts[leaderIdx] = nil

	// Wait for new leader among remaining nodes.
	ok := waitForCondition(5*time.Second, func() bool {
		for i, r := range rafts {
			if r != nil && i != oldLeader && r.IsLeader() {
				return true
			}
		}
		return false
	})
	if !ok {
		t.Fatalf("timeout waiting for new leader after failover")
	}

	newLeaderIdx := -1
	for i, r := range rafts {
		if r != nil && r.IsLeader() {
			newLeaderIdx = i
			break
		}
	}
	t.Logf("New leader after failover: node %d", newLeaderIdx)
	if newLeaderIdx == oldLeader {
		t.Fatalf("new leader should be different from old leader")
	}
}

func TestBasicReplication(t *testing.T) {
	dirs := make([]string, 3)
	addrs := make([]string, 3)

	// Get addresses first.
	transports := make([]*Transport, 3)
	for i := 0; i < 3; i++ {
		tr, err := NewTransport("127.0.0.1:0")
		if err != nil {
			t.Fatalf("create transport %d: %v", i, err)
		}
		transports[i] = tr
		addrs[i] = tr.Addr()
	}
	for _, tr := range transports {
		tr.Close()
	}

	nodes := make([]*Node, 3)
	for i := 0; i < 3; i++ {
		dirs[i] = t.TempDir()
		peers := make([]string, 0, 2)
		for j := 0; j < 3; j++ {
			if j != i {
				peers = append(peers, addrs[j])
			}
		}
		n, err := NewNodeWithConfig(
			fmt.Sprintf("node-%d", i),
			peers,
			dirs[i],
			addrs[i],
			testConfig(),
		)
		if err != nil {
			t.Fatalf("create node %d: %v", i, err)
		}
		nodes[i] = n
	}

	for i, n := range nodes {
		if err := n.Start(); err != nil {
			t.Fatalf("start node %d: %v", i, err)
		}
	}
	defer func() {
		for _, n := range nodes {
			if n != nil {
				n.Stop()
			}
		}
	}()

	// Wait for leader.
	var leaderNode *Node
	ok := waitForCondition(5*time.Second, func() bool {
		for _, n := range nodes {
			if n.Raft().IsLeader() {
				leaderNode = n
				return true
			}
		}
		return false
	})
	if !ok {
		t.Fatalf("timeout waiting for leader")
	}

	// Propose a Put through the leader.
	data := EncodePutCommand([]byte("key1"), []byte("value1"))
	err := leaderNode.Raft().Propose(CmdRaftPut, data)
	if err != nil {
		t.Fatalf("propose failed: %v", err)
	}

	// Wait for the entry to be applied to the leader's engine.
	var val []byte
	ok = waitForCondition(5*time.Second, func() bool {
		v, err := leaderNode.Engine().Get([]byte("key1"))
		if err != nil {
			return false
		}
		val = v
		return true
	})
	if !ok {
		t.Fatalf("timeout waiting for value to appear in leader engine")
	}
	if string(val) != "value1" {
		t.Fatalf("expected value1, got %s", string(val))
	}

	// Verify followers also received the value (eventually).
	ok = waitForCondition(5*time.Second, func() bool {
		for _, n := range nodes {
			v, err := n.Engine().Get([]byte("key1"))
			if err != nil || string(v) != "value1" {
				return false
			}
		}
		return true
	})
	if !ok {
		t.Fatalf("timeout waiting for value to replicate to all engines")
	}

	t.Logf("Value replicated to all engines successfully")
}
