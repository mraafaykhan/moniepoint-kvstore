package server

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/raafay/kvstore/engine"
	"github.com/raafay/kvstore/raft"
)

// NodeMetrics tracks per-node read/write metrics.
type NodeMetrics struct {
	WritesTotal  atomic.Int64
	ReadsTotal   atomic.Int64
	lastWrites   int64
	lastReads    int64
	WritesPerSec float64
	ReadsPerSec  float64
}

// NodeInfo describes the status of a single cluster node.
type NodeInfo struct {
	ID            string  `json:"id"`
	State         string  `json:"state"`
	Term          uint64  `json:"term"`
	CommitIndex   uint64  `json:"commitIndex"`
	LogLength     uint64  `json:"logLength"`
	LeaderID      string  `json:"leaderId"`
	KeyCount      int64   `json:"keyCount"`
	Addr          string  `json:"addr"`
	WritesPerSec  float64 `json:"writesPerSec"`
	ReadsPerSec   float64 `json:"readsPerSec"`
	MemtableBytes int     `json:"memtableBytes"`
	SSTables      int     `json:"sstables"`
}

// ClusterStatus describes the overall cluster state.
type ClusterStatus struct {
	Running   bool       `json:"running"`
	Nodes     []NodeInfo `json:"nodes"`
	Timestamp int64      `json:"timestamp"` // unix ms for frontend graphing
	NodeCount int        `json:"nodeCount"`
}

// ClusterManager manages an in-process Raft cluster for the dashboard demo.
type ClusterManager struct {
	mu          sync.RWMutex
	nodes       []*raft.Node
	stopped     map[string]bool    // tracks which node IDs have been stopped
	running     bool
	baseDir     string
	events      *EventBus
	keyCount    atomic.Int64
	closeCh     chan struct{}
	wg          sync.WaitGroup
	nodeMetrics map[string]*NodeMetrics // per-node metrics
	nodeCount   int                     // configurable node count
}

// NewClusterManager creates a new ClusterManager.
func NewClusterManager(events *EventBus) *ClusterManager {
	return &ClusterManager{
		events:  events,
		stopped: make(map[string]bool),
	}
}

// IsRunning returns whether the cluster is currently running.
func (cm *ClusterManager) IsRunning() bool {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.running
}

// Start creates a cluster with the default node count of 3.
func (cm *ClusterManager) Start() error {
	return cm.StartWithNodes(3)
}

// StartWithNodes creates a temporary directory, creates the specified number of nodes, and starts them.
// Count is clamped to [1, 7].
func (cm *ClusterManager) StartWithNodes(count int) error {
	if count < 1 {
		count = 1
	}
	if count > 7 {
		count = 7
	}

	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.running {
		return nil // idempotent — already running is fine
	}

	dir, err := os.MkdirTemp("", "kvcluster-*")
	if err != nil {
		return fmt.Errorf("cluster: create temp dir: %w", err)
	}
	cm.baseDir = dir

	// Pick a random base port in 17000-17900 range to avoid conflicts.
	basePort := 17000 + rand.Intn(900)

	// Build address list for all nodes.
	addrs := make([]string, count)
	for i := 0; i < count; i++ {
		addrs[i] = fmt.Sprintf("127.0.0.1:%d", basePort+i)
	}

	config := raft.Config{
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
		HeartbeatInterval:  50 * time.Millisecond,
		ProposalTimeout:    5 * time.Second,
	}

	cm.nodes = make([]*raft.Node, count)
	cm.nodeMetrics = make(map[string]*NodeMetrics, count)
	for i := 0; i < count; i++ {
		nodeID := fmt.Sprintf("node-%d", i)
		nodeDir := filepath.Join(dir, nodeID)

		// Build peer list (all addrs except this node's).
		var peers []string
		for j := 0; j < count; j++ {
			if j != i {
				peers = append(peers, addrs[j])
			}
		}

		node, err := raft.NewNodeWithConfig(nodeID, peers, nodeDir, addrs[i], config)
		if err != nil {
			// Clean up already-created nodes.
			for k := 0; k < i; k++ {
				cm.nodes[k].Stop()
			}
			os.RemoveAll(dir)
			return fmt.Errorf("cluster: create node %d: %w", i, err)
		}
		cm.nodes[i] = node
		cm.nodeMetrics[nodeID] = &NodeMetrics{}
	}

	// Start all nodes.
	for i, node := range cm.nodes {
		if err := node.Start(); err != nil {
			// Stop previously started nodes.
			for k := 0; k < i; k++ {
				cm.nodes[k].Stop()
			}
			os.RemoveAll(dir)
			return fmt.Errorf("cluster: start node %d: %w", i, err)
		}
	}

	cm.running = true
	cm.nodeCount = count
	cm.stopped = make(map[string]bool)
	cm.keyCount.Store(0)
	cm.closeCh = make(chan struct{})

	// Start background status publisher.
	cm.wg.Add(1)
	go cm.statusLoop()

	cm.events.Publish(Event{
		Type: "cluster_started",
		Data: map[string]interface{}{"nodeCount": count},
		Time: time.Now(),
	})

	return nil
}

// Stop shuts down all nodes and cleans up temporary directories.
func (cm *ClusterManager) Stop() error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if !cm.running {
		return errors.New("cluster not running")
	}

	close(cm.closeCh)
	cm.wg.Wait()

	var firstErr error
	for _, node := range cm.nodes {
		if node == nil {
			continue
		}
		if err := node.Stop(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	if cm.baseDir != "" {
		os.RemoveAll(cm.baseDir)
		cm.baseDir = ""
	}

	cm.nodes = nil
	cm.running = false
	cm.stopped = make(map[string]bool)

	cm.events.Publish(Event{
		Type: "cluster_stopped",
		Data: nil,
		Time: time.Now(),
	})

	return firstErr
}

// Status returns the current status of the cluster and all nodes.
func (cm *ClusterManager) Status() ClusterStatus {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	status := ClusterStatus{
		Running:   cm.running,
		Timestamp: time.Now().UnixMilli(),
		NodeCount: cm.nodeCount,
	}
	if !cm.running {
		return status
	}

	keyCount := cm.keyCount.Load()
	for _, node := range cm.nodes {
		if node == nil {
			continue
		}
		rs := node.Raft().Status()
		eStats := node.Engine().Stats()
		ni := NodeInfo{
			ID:            rs.NodeID,
			State:         rs.State,
			Term:          rs.Term,
			CommitIndex:   rs.CommitIndex,
			LogLength:     rs.LogLength,
			LeaderID:      rs.LeaderID,
			KeyCount:      keyCount,
			Addr:          node.Raft().TransportAddr(),
			MemtableBytes: eStats.MemoryBytes,
			SSTables:      eStats.TotalSSTables,
		}
		if m, ok := cm.nodeMetrics[rs.NodeID]; ok {
			ni.WritesPerSec = m.WritesPerSec
			ni.ReadsPerSec = m.ReadsPerSec
		}
		if cm.stopped[rs.NodeID] {
			ni.State = "Stopped"
		}
		status.Nodes = append(status.Nodes, ni)
	}

	return status
}

// findLeader scans nodes and returns the current leader.
// If no leader yet (election in progress), briefly waits up to 2s.
func (cm *ClusterManager) findLeader() (*raft.Node, error) {
	// Fast path: scan under existing lock
	for _, node := range cm.nodes {
		if node == nil {
			continue
		}
		if cm.stopped[node.Raft().NodeID()] {
			continue
		}
		if node.Raft().IsLeader() {
			return node, nil
		}
	}

	// No leader found — release lock and wait for election
	nodes := make([]*raft.Node, len(cm.nodes))
	copy(nodes, cm.nodes)
	stoppedCopy := make(map[string]bool, len(cm.stopped))
	for k, v := range cm.stopped {
		stoppedCopy[k] = v
	}
	cm.mu.RUnlock()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
		for _, node := range nodes {
			if node == nil {
				continue
			}
			if stoppedCopy[node.Raft().NodeID()] {
				continue
			}
			if node.Raft().IsLeader() {
				cm.mu.RLock() // re-acquire before returning
				return node, nil
			}
		}
	}
	cm.mu.RLock() // re-acquire before returning
	return nil, errors.New("no leader elected within 2s")
}

// Put inserts a key-value pair through the Raft leader.
func (cm *ClusterManager) Put(key, value string) error {
	cm.mu.RLock()
	if !cm.running {
		cm.mu.RUnlock()
		return errors.New("cluster not running")
	}
	leader, err := cm.findLeader()
	cm.mu.RUnlock()
	if err != nil {
		return err
	}

	data := raft.EncodePutCommand([]byte(key), []byte(value))
	if err := leader.Raft().Propose(raft.CmdRaftPut, data); err != nil {
		return err
	}

	leaderID := leader.Raft().NodeID()
	if m, ok := cm.nodeMetrics[leaderID]; ok {
		m.WritesTotal.Add(1)
	}
	cm.keyCount.Add(1)
	return nil
}

// Get reads a value from the leader's engine.
func (cm *ClusterManager) Get(key string) (string, bool, error) {
	cm.mu.RLock()
	if !cm.running {
		cm.mu.RUnlock()
		return "", false, errors.New("cluster not running")
	}
	leader, err := cm.findLeader()
	cm.mu.RUnlock()
	if err != nil {
		return "", false, err
	}

	val, err := leader.Engine().Get([]byte(key))
	leaderID := leader.Raft().NodeID()
	if m, ok := cm.nodeMetrics[leaderID]; ok {
		m.ReadsTotal.Add(1)
	}
	if err != nil {
		if errors.Is(err, engine.ErrNotFound) {
			return "", false, nil
		}
		return "", false, err
	}
	return string(val), true, nil
}

// Delete removes a key through the Raft leader.
func (cm *ClusterManager) Delete(key string) error {
	cm.mu.RLock()
	if !cm.running {
		cm.mu.RUnlock()
		return errors.New("cluster not running")
	}
	leader, err := cm.findLeader()
	cm.mu.RUnlock()
	if err != nil {
		return err
	}

	data := raft.EncodeDeleteCommand([]byte(key))
	if err := leader.Raft().Propose(raft.CmdRaftDelete, data); err != nil {
		return err
	}

	leaderID := leader.Raft().NodeID()
	if m, ok := cm.nodeMetrics[leaderID]; ok {
		m.WritesTotal.Add(1)
	}
	cm.keyCount.Add(-1)
	return nil
}

// BatchPut inserts multiple key-value pairs through the Raft leader.
func (cm *ClusterManager) BatchPut(keys, values []string) error {
	cm.mu.RLock()
	if !cm.running {
		cm.mu.RUnlock()
		return errors.New("cluster not running")
	}
	leader, err := cm.findLeader()
	cm.mu.RUnlock()
	if err != nil {
		return err
	}

	if len(keys) != len(values) {
		return errors.New("keys and values length mismatch")
	}

	bKeys := make([][]byte, len(keys))
	bValues := make([][]byte, len(values))
	for i := range keys {
		bKeys[i] = []byte(keys[i])
		bValues[i] = []byte(values[i])
	}

	data := raft.EncodeBatchPutCommand(bKeys, bValues)
	if err := leader.Raft().Propose(raft.CmdRaftBatchPut, data); err != nil {
		return err
	}

	leaderID := leader.Raft().NodeID()
	if m, ok := cm.nodeMetrics[leaderID]; ok {
		m.WritesTotal.Add(int64(len(keys)))
	}
	cm.keyCount.Add(int64(len(keys)))
	return nil
}

// GetRange reads a range of keys from the leader's engine.
func (cm *ClusterManager) GetRange(start, end string) ([]engine.KVPair, error) {
	cm.mu.RLock()
	if !cm.running {
		cm.mu.RUnlock()
		return nil, errors.New("cluster not running")
	}
	leader, err := cm.findLeader()
	cm.mu.RUnlock()
	if err != nil {
		return nil, err
	}

	pairs, err := leader.Engine().ReadRange([]byte(start), []byte(end))
	leaderID := leader.Raft().NodeID()
	if m, ok := cm.nodeMetrics[leaderID]; ok {
		m.ReadsTotal.Add(1)
	}
	if err != nil {
		return nil, err
	}
	return pairs, nil
}

// StopNode stops a specific node by ID (for failover demo).
func (cm *ClusterManager) StopNode(id string) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if !cm.running {
		return errors.New("cluster not running")
	}

	for _, node := range cm.nodes {
		if node == nil {
			continue
		}
		if node.Raft().NodeID() == id {
			if cm.stopped[id] {
				return fmt.Errorf("node %s already stopped", id)
			}
			if err := node.Stop(); err != nil {
				return err
			}
			cm.stopped[id] = true

			cm.events.Publish(Event{
				Type: "node_stopped",
				Data: map[string]interface{}{"id": id},
				Time: time.Now(),
			})
			return nil
		}
	}
	return fmt.Errorf("node %s not found", id)
}

// statusLoop periodically publishes a status event for real-time frontend updates.
func (cm *ClusterManager) statusLoop() {
	defer cm.wg.Done()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-cm.closeCh:
			return
		case <-ticker.C:
			// Compute per-second rates (×2 because 500ms interval).
			cm.mu.RLock()
			for _, m := range cm.nodeMetrics {
				currentWrites := m.WritesTotal.Load()
				currentReads := m.ReadsTotal.Load()
				m.WritesPerSec = float64(currentWrites-m.lastWrites) * 2
				m.ReadsPerSec = float64(currentReads-m.lastReads) * 2
				m.lastWrites = currentWrites
				m.lastReads = currentReads
			}
			cm.mu.RUnlock()

			status := cm.Status()
			cm.events.Publish(Event{
				Type: "status",
				Data: status,
				Time: time.Now(),
			})
		}
	}
}
