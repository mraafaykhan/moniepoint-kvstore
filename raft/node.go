package raft

import (
	"fmt"
	"path/filepath"

	"github.com/raafay/kvstore/engine"
)

// Node integrates a Raft instance with the KV storage engine.
type Node struct {
	raft   *Raft
	eng    *engine.Engine
	dir    string
	closeCh chan struct{}
}

// NewNode creates a new Raft-backed node.
func NewNode(nodeID string, peers []string, dir string, raftAddr string) (*Node, error) {
	return NewNodeWithConfig(nodeID, peers, dir, raftAddr, DefaultConfig())
}

// NewNodeWithConfig creates a new Raft-backed node with custom timing configuration.
func NewNodeWithConfig(nodeID string, peers []string, dir string, raftAddr string, config Config) (*Node, error) {
	raftDir := filepath.Join(dir, "raft")
	r, err := NewRaftWithConfig(nodeID, peers, raftDir, raftAddr, config)
	if err != nil {
		return nil, fmt.Errorf("node: create raft: %w", err)
	}

	return &Node{
		raft:    r,
		dir:     dir,
		closeCh: make(chan struct{}),
	}, nil
}

// Start opens the engine, starts the Raft protocol, and begins the apply loop.
func (n *Node) Start() error {
	eng, err := engine.Open(engine.Options{
		Dir: filepath.Join(n.dir, "data"),
	})
	if err != nil {
		return fmt.Errorf("node: open engine: %w", err)
	}
	n.eng = eng

	if err := n.raft.Start(); err != nil {
		eng.Close()
		return fmt.Errorf("node: start raft: %w", err)
	}

	// Start the engine apply loop.
	go n.engineApplyLoop()

	return nil
}

// engineApplyLoop reads committed entries from Raft and applies them to the engine.
func (n *Node) engineApplyLoop() {
	ch := n.raft.ApplyCh()
	for {
		select {
		case entry := <-ch:
			n.applyEntry(entry)
		case <-n.closeCh:
			return
		}
	}
}

func (n *Node) applyEntry(entry LogEntry) {
	switch entry.CmdType {
	case CmdRaftPut:
		key, value := DecodePutCommand(entry.Data)
		n.eng.Put(key, value)
	case CmdRaftDelete:
		key := DecodeDeleteCommand(entry.Data)
		n.eng.Delete(key)
	case CmdRaftBatchPut:
		keys, values := DecodeBatchPutCommand(entry.Data)
		n.eng.BatchPut(keys, values)
	}
}

// Stop shuts down the node gracefully.
func (n *Node) Stop() error {
	close(n.closeCh)
	var firstErr error
	if err := n.raft.Stop(); err != nil && firstErr == nil {
		firstErr = err
	}
	if n.eng != nil {
		if err := n.eng.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Engine returns the underlying storage engine.
func (n *Node) Engine() *engine.Engine {
	return n.eng
}

// Raft returns the underlying Raft instance.
func (n *Node) Raft() *Raft {
	return n.raft
}
