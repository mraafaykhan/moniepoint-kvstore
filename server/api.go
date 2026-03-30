package server

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"math/rand"
	"net"
	"net/http"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/raafay/kvstore/engine"
)

// APIServer serves the JSON HTTP API and web dashboard.
type APIServer struct {
	engine  *engine.Engine
	cluster *ClusterManager
	events  *EventBus
	mux     *http.ServeMux
	server  *http.Server
}

//go:embed dashboard
var dashboardFS embed.FS

// NewAPIServer creates a new API server bound to the given address.
func NewAPIServer(addr string, eng *engine.Engine) *APIServer {
	events := NewEventBus()
	s := &APIServer{
		engine:  eng,
		cluster: NewClusterManager(events),
		events:  events,
		mux:     http.NewServeMux(),
	}

	// KV operation endpoints.
	s.mux.HandleFunc("/api/put", s.cors(s.handlePut))
	s.mux.HandleFunc("/api/get", s.cors(s.handleGet))
	s.mux.HandleFunc("/api/delete", s.cors(s.handleDelete))
	s.mux.HandleFunc("/api/batch", s.cors(s.handleBatch))
	s.mux.HandleFunc("/api/batch-put", s.cors(s.handleBatch))
	s.mux.HandleFunc("/api/range", s.cors(s.handleRange))

	// Benchmark endpoints (both naming conventions).
	s.mux.HandleFunc("/api/benchmark/writes", s.cors(s.handleBenchWrites))
	s.mux.HandleFunc("/api/benchmark/seq-write", s.cors(s.handleBenchWrites))
	s.mux.HandleFunc("/api/benchmark/reads", s.cors(s.handleBenchReads))
	s.mux.HandleFunc("/api/benchmark/rand-read", s.cors(s.handleBenchReads))
	s.mux.HandleFunc("/api/benchmark/batch", s.cors(s.handleBenchBatch))
	s.mux.HandleFunc("/api/benchmark/batch-write", s.cors(s.handleBenchBatch))
	s.mux.HandleFunc("/api/benchmark/range", s.cors(s.handleBenchRange))
	s.mux.HandleFunc("/api/benchmark/range-scan", s.cors(s.handleBenchRange))
	s.mux.HandleFunc("/api/benchmark/mixed", s.cors(s.handleBenchMixed))
	s.mux.HandleFunc("/api/benchmark/crash-recovery", s.cors(s.handleCrashRecovery))
	s.mux.HandleFunc("/api/demo/crash-recovery", s.cors(s.handleCrashRecovery))

	// System info.
	s.mux.HandleFunc("/api/info", s.cors(s.handleInfo))

	// Cluster endpoints.
	s.mux.HandleFunc("/api/cluster/start", s.cors(s.handleClusterStart))
	s.mux.HandleFunc("/api/cluster/stop", s.cors(s.handleClusterStop))
	s.mux.HandleFunc("/api/cluster/status", s.cors(s.handleClusterStatus))
	s.mux.HandleFunc("/api/cluster/put", s.cors(s.handleClusterPut))
	s.mux.HandleFunc("/api/cluster/get", s.cors(s.handleClusterGet))
	s.mux.HandleFunc("/api/cluster/delete", s.cors(s.handleClusterDelete))
	s.mux.HandleFunc("/api/cluster/batch", s.cors(s.handleClusterBatch))
	s.mux.HandleFunc("/api/cluster/range", s.cors(s.handleClusterRange))
	s.mux.HandleFunc("/api/cluster/stop-node", s.cors(s.handleClusterStopNode))
	s.mux.HandleFunc("/api/cluster/events", s.handleClusterEvents)

	// Serve embedded dashboard.
	sub, _ := fs.Sub(dashboardFS, "dashboard")
	fileServer := http.FileServer(http.FS(sub))
	s.mux.Handle("/", fileServer)

	s.server = &http.Server{Addr: addr, Handler: s.mux}
	return s
}

// Start begins listening and serving.
func (s *APIServer) Start() error {
	ln, err := net.Listen("tcp", s.server.Addr)
	if err != nil {
		return err
	}
	go s.server.Serve(ln)
	return nil
}

// Stop gracefully shuts down the server.
func (s *APIServer) Stop() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.server.Shutdown(ctx)
}

func (s *APIServer) cors(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next(w, r)
	}
}

func jsonOK(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

func jsonErr(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]interface{}{"ok": false, "error": msg})
}

// ============================================================================
// KV Operations
// ============================================================================

func (s *APIServer) handlePut(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonErr(w, 400, err.Error())
		return
	}
	if req.Key == "" {
		jsonErr(w, 400, "key is required")
		return
	}
	if err := s.engine.Put([]byte(req.Key), []byte(req.Value)); err != nil {
		jsonErr(w, 500, err.Error())
		return
	}
	jsonOK(w, map[string]interface{}{"ok": true})
}

func (s *APIServer) handleGet(w http.ResponseWriter, r *http.Request) {
	var key string
	// Support both GET with query param and POST with JSON body
	if r.Method == http.MethodGet {
		key = r.URL.Query().Get("key")
	} else {
		var req struct {
			Key string `json:"key"`
		}
		json.NewDecoder(r.Body).Decode(&req)
		key = req.Key
	}
	if key == "" {
		jsonErr(w, 400, "key is required")
		return
	}
	val, err := s.engine.Get([]byte(key))
	if err != nil {
		if errors.Is(err, engine.ErrNotFound) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(404)
			json.NewEncoder(w).Encode(map[string]interface{}{"ok": false, "found": false, "error": "not found"})
			return
		}
		jsonErr(w, 500, err.Error())
		return
	}
	jsonOK(w, map[string]interface{}{"ok": true, "found": true, "value": string(val)})
}

func (s *APIServer) handleDelete(w http.ResponseWriter, r *http.Request) {
	var key string
	if r.Method == http.MethodGet || r.Method == http.MethodDelete {
		key = r.URL.Query().Get("key")
	} else {
		var req struct {
			Key string `json:"key"`
		}
		json.NewDecoder(r.Body).Decode(&req)
		key = req.Key
	}
	if key == "" {
		jsonErr(w, 400, "key is required")
		return
	}
	if err := s.engine.Delete([]byte(key)); err != nil {
		jsonErr(w, 500, err.Error())
		return
	}
	jsonOK(w, map[string]interface{}{"ok": true})
}

func (s *APIServer) handleBatch(w http.ResponseWriter, r *http.Request) {
	var req struct {
		// Support both {keys:[], values:[]} and {pairs:[{key,value}]}
		Keys   []string `json:"keys"`
		Values []string `json:"values"`
		Pairs  []struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		} `json:"pairs"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonErr(w, 400, err.Error())
		return
	}

	var keys, values [][]byte
	if len(req.Keys) > 0 {
		if len(req.Keys) != len(req.Values) {
			jsonErr(w, 400, "keys and values must have equal length")
			return
		}
		keys = make([][]byte, len(req.Keys))
		values = make([][]byte, len(req.Values))
		for i := range req.Keys {
			keys[i] = []byte(req.Keys[i])
			values[i] = []byte(req.Values[i])
		}
	} else if len(req.Pairs) > 0 {
		keys = make([][]byte, len(req.Pairs))
		values = make([][]byte, len(req.Pairs))
		for i, p := range req.Pairs {
			keys[i] = []byte(p.Key)
			values[i] = []byte(p.Value)
		}
	} else {
		jsonErr(w, 400, "keys/values or pairs required")
		return
	}

	if err := s.engine.BatchPut(keys, values); err != nil {
		jsonErr(w, 500, err.Error())
		return
	}
	jsonOK(w, map[string]interface{}{"ok": true, "count": len(keys)})
}

func (s *APIServer) handleRange(w http.ResponseWriter, r *http.Request) {
	var start, end string
	if r.Method == http.MethodGet {
		start = r.URL.Query().Get("start")
		end = r.URL.Query().Get("end")
	} else {
		var req struct {
			Start string `json:"start"`
			End   string `json:"end"`
		}
		json.NewDecoder(r.Body).Decode(&req)
		start = req.Start
		end = req.End
	}
	if start == "" || end == "" {
		jsonErr(w, 400, "start and end are required")
		return
	}
	pairs, err := s.engine.ReadRange([]byte(start), []byte(end))
	if err != nil {
		jsonErr(w, 500, err.Error())
		return
	}
	type kv struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	out := make([]kv, len(pairs))
	for i, p := range pairs {
		out[i] = kv{Key: string(p.Key), Value: string(p.Value)}
	}
	jsonOK(w, map[string]interface{}{"ok": true, "pairs": out, "count": len(out)})
}

// ============================================================================
// Benchmarks — run through cluster if running, otherwise use main engine.
// All benchmarks emit events to the event feed.
// ============================================================================

func randValue(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte('a' + rand.Intn(26))
	}
	return b
}

func bkey(prefix string, i int) []byte {
	return []byte(fmt.Sprintf("%s-%08d", prefix, i))
}

// benchEngine returns the engine to benchmark against.
// If cluster is running, returns the leader's engine directly (bypasses Raft
// consensus for raw throughput — we're measuring engine speed, not replication).
// Metrics are still tracked on the cluster.
func (s *APIServer) benchEngine() *engine.Engine {
	if s.cluster.IsRunning() {
		s.cluster.mu.RLock()
		leader, err := s.cluster.findLeader()
		s.cluster.mu.RUnlock()
		if err == nil {
			return leader.Engine()
		}
	}
	return s.engine
}

func (s *APIServer) benchPut(key, val string) error {
	return s.benchEngine().Put([]byte(key), []byte(val))
}

func (s *APIServer) benchGet(key string) error {
	_, err := s.benchEngine().Get([]byte(key))
	return err
}

func (s *APIServer) benchBatchPut(keys, vals []string) error {
	eng := s.benchEngine()
	bk := make([][]byte, len(keys))
	bv := make([][]byte, len(vals))
	for i := range keys {
		bk[i] = []byte(keys[i])
		bv[i] = []byte(vals[i])
	}
	return eng.BatchPut(bk, bv)
}

func (s *APIServer) benchRange(start, end string) error {
	_, err := s.benchEngine().ReadRange([]byte(start), []byte(end))
	return err
}

func (s *APIServer) handleBenchWrites(w http.ResponseWriter, r *http.Request) {
	count := 10000
	batchSize := 100
	pfx := fmt.Sprintf("bw-%d", time.Now().UnixNano())
	val := string(randValue(512))

	via := "engine"
	if s.cluster.IsRunning() {
		via = "engine (leader)"
	}
	s.events.Publish(Event{Type: "benchmark", Data: fmt.Sprintf("Starting writes benchmark (%d ops in batches of %d via %s)", count, batchSize, via), Time: time.Now()})

	start := time.Now()
	for i := 0; i < count; i += batchSize {
		sz := batchSize
		if i+sz > count {
			sz = count - i
		}
		keys := make([]string, sz)
		vals := make([]string, sz)
		for j := 0; j < sz; j++ {
			keys[j] = string(bkey(pfx, i+j))
			vals[j] = val
		}
		if err := s.benchBatchPut(keys, vals); err != nil {
			jsonErr(w, 500, err.Error())
			return
		}
	}
	elapsed := time.Since(start)
	opsPerSec := float64(count) / elapsed.Seconds()

	s.events.Publish(Event{Type: "benchmark", Data: fmt.Sprintf("Writes: %.0f ops/sec (%d keys via %s)", opsPerSec, count, via), Time: time.Now()})

	jsonOK(w, map[string]interface{}{
		"opsPerSec":    opsPerSec,
		"avgLatencyUs": float64(elapsed.Microseconds()) / float64(count),
		"totalMs":      float64(elapsed.Milliseconds()),
		"count":        count,
	})
}

func (s *APIServer) handleBenchReads(w http.ResponseWriter, r *http.Request) {
	count := 10000

	via := "engine"
	if s.cluster.IsRunning() {
		via = "engine (leader)"
	}

	// Pre-populate with larger values in batches
	pfx := fmt.Sprintf("br-%d", time.Now().UnixNano())
	val := string(randValue(512))
	s.events.Publish(Event{Type: "benchmark", Data: fmt.Sprintf("Pre-populating %d keys (512B each) via %s...", count, via), Time: time.Now()})
	for i := 0; i < count; i += 50 {
		sz := 50
		if i+sz > count {
			sz = count - i
		}
		ks := make([]string, sz)
		vs := make([]string, sz)
		for j := 0; j < sz; j++ {
			ks[j] = string(bkey(pfx, i+j))
			vs[j] = val
		}
		s.benchBatchPut(ks, vs)
	}

	s.events.Publish(Event{Type: "benchmark", Data: fmt.Sprintf("Starting random reads benchmark (%d ops via %s)", count, via), Time: time.Now()})
	start := time.Now()
	for i := 0; i < count; i++ {
		s.benchGet(string(bkey(pfx, rand.Intn(count))))
	}
	elapsed := time.Since(start)
	opsPerSec := float64(count) / elapsed.Seconds()

	s.events.Publish(Event{Type: "benchmark", Data: fmt.Sprintf("Random reads: %.0f ops/sec via %s", opsPerSec, via), Time: time.Now()})

	jsonOK(w, map[string]interface{}{
		"opsPerSec":    opsPerSec,
		"avgLatencyUs": float64(elapsed.Microseconds()) / float64(count),
		"totalMs":      float64(elapsed.Milliseconds()),
		"count":        count,
	})
}

func (s *APIServer) handleBenchBatch(w http.ResponseWriter, r *http.Request) {
	count := 10000
	batchSize := 200

	via := "engine"
	if s.cluster.IsRunning() {
		via = "engine (leader)"
	}
	s.events.Publish(Event{Type: "benchmark", Data: fmt.Sprintf("Starting batch writes benchmark (%d ops, batch size %d, via %s)", count, batchSize, via), Time: time.Now()})

	pfx := fmt.Sprintf("bb-%d", time.Now().UnixNano())
	val := string(randValue(512))

	start := time.Now()
	for written := 0; written < count; written += batchSize {
		sz := batchSize
		if written+sz > count {
			sz = count - written
		}
		keys := make([]string, sz)
		vals := make([]string, sz)
		for i := 0; i < sz; i++ {
			keys[i] = string(bkey(pfx, written+i))
			vals[i] = val
		}
		if err := s.benchBatchPut(keys, vals); err != nil {
			jsonErr(w, 500, err.Error())
			return
		}
	}
	elapsed := time.Since(start)
	opsPerSec := float64(count) / elapsed.Seconds()

	s.events.Publish(Event{Type: "benchmark", Data: fmt.Sprintf("Batch writes: %.0f ops/sec via %s", opsPerSec, via), Time: time.Now()})

	jsonOK(w, map[string]interface{}{
		"opsPerSec":    opsPerSec,
		"avgLatencyUs": float64(elapsed.Microseconds()) / float64(count),
		"totalMs":      float64(elapsed.Milliseconds()),
		"count":        count,
	})
}

func (s *APIServer) handleBenchRange(w http.ResponseWriter, r *http.Request) {
	scanCount := 500
	rangeSize := 100
	totalKeys := 10000

	via := "engine"
	if s.cluster.IsRunning() {
		via = "engine (leader)"
	}

	// Pre-populate
	pfx := fmt.Sprintf("brs-%d", time.Now().UnixNano())
	val := string(randValue(512))
	s.events.Publish(Event{Type: "benchmark", Data: fmt.Sprintf("Pre-populating %d keys for range scan via %s...", totalKeys, via), Time: time.Now()})
	for i := 0; i < totalKeys; i += 50 {
		sz := 50
		if i+sz > totalKeys {
			sz = totalKeys - i
		}
		ks := make([]string, sz)
		vs := make([]string, sz)
		for j := 0; j < sz; j++ {
			ks[j] = string(bkey(pfx, i+j))
			vs[j] = val
		}
		s.benchBatchPut(ks, vs)
	}

	s.events.Publish(Event{Type: "benchmark", Data: fmt.Sprintf("Starting range scan benchmark (%d scans via %s)", scanCount, via), Time: time.Now()})
	start := time.Now()
	for i := 0; i < scanCount; i++ {
		si := rand.Intn(totalKeys - rangeSize)
		s.benchRange(string(bkey(pfx, si)), string(bkey(pfx, si+rangeSize)))
	}
	elapsed := time.Since(start)
	opsPerSec := float64(scanCount) / elapsed.Seconds()

	s.events.Publish(Event{Type: "benchmark", Data: fmt.Sprintf("Range scans: %.0f scans/sec via %s", opsPerSec, via), Time: time.Now()})

	jsonOK(w, map[string]interface{}{
		"opsPerSec":    opsPerSec,
		"avgLatencyUs": float64(elapsed.Microseconds()) / float64(scanCount),
		"totalMs":      float64(elapsed.Milliseconds()),
		"count":        scanCount,
	})
}

func (s *APIServer) handleBenchMixed(w http.ResponseWriter, r *http.Request) {
	count := 5000

	via := "engine"
	if s.cluster.IsRunning() {
		via = "engine (leader)"
	}

	pfx := fmt.Sprintf("bm-%d", time.Now().UnixNano())
	val := string(randValue(512))

	// Pre-populate for reads in batches
	prepop := count / 2
	s.events.Publish(Event{Type: "benchmark", Data: fmt.Sprintf("Pre-populating %d keys for mixed benchmark via %s...", prepop, via), Time: time.Now()})
	for i := 0; i < prepop; i += 50 {
		sz := 50
		if i+sz > prepop {
			sz = prepop - i
		}
		ks := make([]string, sz)
		vs := make([]string, sz)
		for j := 0; j < sz; j++ {
			ks[j] = string(bkey(pfx, i+j))
			vs[j] = val
		}
		s.benchBatchPut(ks, vs)
	}

	s.events.Publish(Event{Type: "benchmark", Data: fmt.Sprintf("Starting mixed workload (%d ops, 50/50 read/write via %s)", count, via), Time: time.Now()})
	writeIdx := prepop
	writeBatch := make([]string, 0, 20)
	writeBatchV := make([]string, 0, 20)
	totalOps := 0
	start := time.Now()
	for i := 0; i < count; i++ {
		if rand.Float64() < 0.5 {
			// Flush pending writes first
			if len(writeBatch) > 0 {
				s.benchBatchPut(writeBatch, writeBatchV)
				totalOps += len(writeBatch)
				writeBatch = writeBatch[:0]
				writeBatchV = writeBatchV[:0]
			}
			s.benchGet(string(bkey(pfx, rand.Intn(prepop))))
			totalOps++
		} else {
			writeBatch = append(writeBatch, string(bkey(pfx, writeIdx)))
			writeBatchV = append(writeBatchV, val)
			writeIdx++
			if len(writeBatch) >= 20 {
				s.benchBatchPut(writeBatch, writeBatchV)
				totalOps += len(writeBatch)
				writeBatch = writeBatch[:0]
				writeBatchV = writeBatchV[:0]
			}
		}
	}
	if len(writeBatch) > 0 {
		s.benchBatchPut(writeBatch, writeBatchV)
		totalOps += len(writeBatch)
	}
	elapsed := time.Since(start)
	opsPerSec := float64(count) / elapsed.Seconds()

	s.events.Publish(Event{Type: "benchmark", Data: fmt.Sprintf("Mixed workload: %.0f ops/sec via %s", opsPerSec, via), Time: time.Now()})

	jsonOK(w, map[string]interface{}{
		"opsPerSec":    opsPerSec,
		"avgLatencyUs": float64(elapsed.Microseconds()) / float64(count),
		"totalMs":      float64(elapsed.Milliseconds()),
		"count":        count,
	})
}

// ============================================================================
// Crash Recovery Demo
// ============================================================================

func (s *APIServer) handleCrashRecovery(w http.ResponseWriter, r *http.Request) {
	keyCount := 100

	dir, err := os.MkdirTemp("", "kvcrash-*")
	if err != nil {
		jsonErr(w, 500, err.Error())
		return
	}
	defer os.RemoveAll(dir)

	eng, err := engine.Open(engine.Options{Dir: dir, MemtableSize: 4 * 1024 * 1024})
	if err != nil {
		jsonErr(w, 500, err.Error())
		return
	}

	pfx := fmt.Sprintf("crash-%d", time.Now().UnixNano())
	for i := 0; i < keyCount; i++ {
		eng.Put(bkey(pfx, i), randValue(64))
	}
	// Simulate crash: close without graceful flush
	eng.Close()

	// Recovery
	recStart := time.Now()
	eng2, err := engine.Open(engine.Options{Dir: dir, MemtableSize: 4 * 1024 * 1024})
	recTime := time.Since(recStart)
	if err != nil {
		jsonErr(w, 500, err.Error())
		return
	}
	defer eng2.Close()

	recovered := 0
	for i := 0; i < keyCount; i++ {
		if _, err := eng2.Get(bkey(pfx, i)); err == nil {
			recovered++
		}
	}

	jsonOK(w, map[string]interface{}{
		"keysWritten":    keyCount,
		"keysRecovered":  recovered,
		"recoveryTimeMs": float64(recTime.Microseconds()) / 1000.0,
		"dataLost":       recovered < keyCount,
	})
}

// ============================================================================
// System Info
// ============================================================================

func (s *APIServer) handleInfo(w http.ResponseWriter, r *http.Request) {
	jsonOK(w, map[string]interface{}{
		"goVersion": strings.TrimPrefix(runtime.Version(), "go"),
		"os":        runtime.GOOS,
		"arch":      runtime.GOARCH,
		"numCPU":    runtime.NumCPU(),
		"features":  []string{"Put", "Read", "ReadKeyRange", "BatchPut", "Delete", "WAL", "SSTable", "BloomFilter", "Compaction", "Raft"},
	})
}

// ============================================================================
// Cluster Operations
// ============================================================================

func (s *APIServer) handleClusterStart(w http.ResponseWriter, r *http.Request) {
	var req struct {
		NodeCount int `json:"nodeCount"`
	}
	json.NewDecoder(r.Body).Decode(&req) // ignore error, defaults to 0
	if req.NodeCount <= 0 {
		req.NodeCount = 3
	}
	if err := s.cluster.StartWithNodes(req.NodeCount); err != nil {
		jsonErr(w, 500, err.Error())
		return
	}
	jsonOK(w, map[string]interface{}{"ok": true, "nodeCount": req.NodeCount})
}

func (s *APIServer) handleClusterStop(w http.ResponseWriter, r *http.Request) {
	if err := s.cluster.Stop(); err != nil {
		jsonErr(w, 500, err.Error())
		return
	}
	jsonOK(w, map[string]interface{}{"ok": true})
}

func (s *APIServer) handleClusterStatus(w http.ResponseWriter, r *http.Request) {
	status := s.cluster.Status()
	jsonOK(w, status)
}

func (s *APIServer) handleClusterPut(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonErr(w, 400, err.Error())
		return
	}
	if req.Key == "" {
		jsonErr(w, 400, "key is required")
		return
	}
	if err := s.cluster.Put(req.Key, req.Value); err != nil {
		jsonErr(w, 500, err.Error())
		return
	}
	jsonOK(w, map[string]interface{}{"ok": true})
}

func (s *APIServer) handleClusterGet(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Key string `json:"key"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonErr(w, 400, err.Error())
		return
	}
	if req.Key == "" {
		jsonErr(w, 400, "key is required")
		return
	}
	val, found, err := s.cluster.Get(req.Key)
	if err != nil {
		jsonErr(w, 500, err.Error())
		return
	}
	if !found {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(404)
		json.NewEncoder(w).Encode(map[string]interface{}{"ok": false, "found": false, "error": "not found"})
		return
	}
	jsonOK(w, map[string]interface{}{"ok": true, "found": true, "value": val})
}

func (s *APIServer) handleClusterDelete(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Key string `json:"key"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonErr(w, 400, err.Error())
		return
	}
	if req.Key == "" {
		jsonErr(w, 400, "key is required")
		return
	}
	if err := s.cluster.Delete(req.Key); err != nil {
		jsonErr(w, 500, err.Error())
		return
	}
	jsonOK(w, map[string]interface{}{"ok": true})
}

func (s *APIServer) handleClusterBatch(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Keys   []string `json:"keys"`
		Values []string `json:"values"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonErr(w, 400, err.Error())
		return
	}
	if len(req.Keys) == 0 {
		jsonErr(w, 400, "keys are required")
		return
	}
	if len(req.Keys) != len(req.Values) {
		jsonErr(w, 400, "keys and values must have equal length")
		return
	}
	if err := s.cluster.BatchPut(req.Keys, req.Values); err != nil {
		jsonErr(w, 500, err.Error())
		return
	}
	jsonOK(w, map[string]interface{}{"ok": true, "count": len(req.Keys)})
}

func (s *APIServer) handleClusterRange(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Start string `json:"start"`
		End   string `json:"end"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonErr(w, 400, err.Error())
		return
	}
	if req.Start == "" || req.End == "" {
		jsonErr(w, 400, "start and end are required")
		return
	}
	pairs, err := s.cluster.GetRange(req.Start, req.End)
	if err != nil {
		jsonErr(w, 500, err.Error())
		return
	}
	type kv struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	out := make([]kv, len(pairs))
	for i, p := range pairs {
		out[i] = kv{Key: string(p.Key), Value: string(p.Value)}
	}
	jsonOK(w, map[string]interface{}{"ok": true, "pairs": out, "count": len(out)})
}

func (s *APIServer) handleClusterStopNode(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonErr(w, 400, err.Error())
		return
	}
	if req.ID == "" {
		jsonErr(w, 400, "id is required")
		return
	}
	if err := s.cluster.StopNode(req.ID); err != nil {
		jsonErr(w, 500, err.Error())
		return
	}
	jsonOK(w, map[string]interface{}{"ok": true})
}

func (s *APIServer) handleClusterEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	ch := s.events.Subscribe()
	defer s.events.Unsubscribe(ch)

	for {
		select {
		case evt := <-ch:
			data, _ := json.Marshal(evt)
			fmt.Fprintf(w, "data: %s\n\n", data)
			flusher.Flush()
		case <-r.Context().Done():
			return
		}
	}
}
