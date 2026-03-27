# kvstore

A network-available persistent Key/Value store built in Go using only the standard library. Implements an LSM-Tree storage engine with Raft consensus for multi-node replication and automatic failover.

## Architecture

```
Client → TCP Binary Protocol → [Raft Leader] → LSM-Tree Engine
                                                     │
                                        WAL + Memtable (Skip List)
                                                     │
                                            SSTable Levels (L0-L6)
                                            (mmap'd, Bloom filtered)
```

### Components

- **Memtable**: In-memory skip list for recent writes, sorted by key
- **WAL**: Write-ahead log for crash recovery (append-only with CRC32 checksums)
- **SSTable**: Immutable sorted on-disk files with Bloom filters and block-based indexing
- **Engine**: LSM-Tree tying WAL + memtable + SSTables with background compaction
- **Server**: TCP binary protocol (low latency) + HTTP admin API
- **Raft**: Consensus protocol for multi-node replication and automatic failover

### API

| Operation | Description |
|-----------|-------------|
| `Put(key, value)` | Insert or update a key-value pair |
| `Read(key)` | Read the value for a key |
| `ReadKeyRange(startKey, endKey)` | Read all key-value pairs in a range |
| `BatchPut(keys, values)` | Insert multiple key-value pairs atomically |
| `Delete(key)` | Delete a key |

## Building

```bash
go build ./...
```

## Running

### Single Node

```bash
# Start the server
go run ./cmd/server -dir ./data -tcp :9000 -http :9001

# Open the web dashboard in your browser
open http://localhost:9001
```

The web dashboard at `http://localhost:9001` provides:
- **Live Operations Panel** — Interactive Put, Read, Delete, BatchPut, ReadKeyRange
- **Performance Benchmarks** — Visual metrics with bar charts (ops/sec, latency)
- **Crash Recovery Demo** — Simulate crash and verify zero data loss
- **Requirements Checklist** — Visual verification of all PDF requirements
- **Architecture Diagram** — Visual overview of the system design

### Multi-Node Cluster (Raft)

```bash
# Node 1
go run ./cmd/server -dir ./data1 -tcp :9000 -http :9001 \
  -raft -raft-addr :8000 -node-id node1 \
  -peers :8001,:8002

# Node 2
go run ./cmd/server -dir ./data2 -tcp :9010 -http :9011 \
  -raft -raft-addr :8001 -node-id node2 \
  -peers :8000,:8002

# Node 3
go run ./cmd/server -dir ./data3 -tcp :9020 -http :9021 \
  -raft -raft-addr :8002 -node-id node3 \
  -peers :8000,:8001
```

### CLI Client

```bash
# Put a key
go run ./cmd/client -addr localhost:9000 put mykey myvalue

# Get a key
go run ./cmd/client -addr localhost:9000 get mykey

# Delete a key
go run ./cmd/client -addr localhost:9000 delete mykey

# Range scan
go run ./cmd/client -addr localhost:9000 scan startkey endkey

# Batch put
go run ./cmd/client -addr localhost:9000 batch key1 val1 key2 val2
```

### HTTP Admin

```bash
# Health check
curl http://localhost:9001/health

# Server stats
curl http://localhost:9001/stats
```

## Testing

```bash
# Run all tests
go test ./...

# Run with verbose output
go test -v ./...

# Run specific package
go test -v ./engine/
go test -v ./raft/
```

## Benchmarks

```bash
# Run benchmark (default: 100K keys, 100 byte values)
go run ./cmd/bench

# Custom benchmark
go run ./cmd/bench -n 1000000 -vsize 256
```

## Design Decisions

### Storage Engine: LSM-Tree

Chosen over Bitcask because:
- **Range queries**: SSTables are sorted, enabling efficient `ReadKeyRange` via merge iterators
- **Datasets larger than RAM**: Only memtable + Bloom filters need memory; SSTables are mmap'd
- Battle-tested design (LevelDB, RocksDB, BadgerDB)

### Key Trade-offs

| Decision | Rationale |
|----------|-----------|
| Skip list over RB-tree | Simpler to implement, equivalent O(log n), good cache locality |
| mmap for SSTable reads | OS manages page cache, handles larger-than-RAM naturally |
| Leveled compaction | Better read amplification, more predictable space usage |
| Binary TCP protocol | Eliminates HTTP header overhead (~500 bytes/request) for low latency |
| Raft over Paxos | Easier to implement correctly, well-documented, industry standard |

### Wire Protocol

Binary TCP protocol with minimal overhead:
- Request: `[Length:4][Command:1][Payload]`
- Response: `[Length:4][Status:1][Payload]`
- All integers little-endian

### Crash Recovery

1. Replay MANIFEST to know which SSTables exist
2. Open all SSTable readers
3. Replay WAL to rebuild memtable
4. Ready to serve (typically < 1 second)

## Project Structure

```
kvstore/
├── wal/          # Write-ahead log
├── memtable/     # Skip list + memtable
├── sstable/      # Writer, reader (mmap), bloom filter, iterator
├── engine/       # LSM-Tree engine, compaction, manifest
├── server/       # TCP + HTTP servers
├── raft/         # Raft consensus
├── client/       # Go client library
└── cmd/          # Server, client CLI, benchmarks
```

## References

- [Bigtable](https://static.googleusercontent.com/media/research.google.com/en//archive/bigtable-osdi06.pdf) — Google's distributed storage
- [Bitcask](https://riak.com/assets/bitcask-intro.pdf) — Log-structured hash table
- [LSM-Tree](https://www.cs.umb.edu/~poneil/lsmtree.pdf) — Log-Structured Merge Tree
- [Raft](https://web.stanford.edu/~ouster/cgi-bin/papers/raft-atc14.pdf) — Consensus protocol
- [Paxos](https://lamport.azurewebsites.net/pubs/paxos-simple.pdf) — Consensus protocol
