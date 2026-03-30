# KVStore: Complete Concept Deep-Dive & Study Guide

## How to Use This Document
Read each concept section in order. Each one builds on the previous. For each concept you'll find: what it is in plain English, how it works step-by-step, why we chose it, where it lives in our code, and what to read next for deeper understanding.

---

# PART 1: STORAGE ENGINE CONCEPTS

---

## 1. LSM-Tree (Log-Structured Merge-Tree)

### What is it?
An LSM-Tree is a data structure designed for workloads where writes vastly outnumber reads. Instead of updating data in-place on disk (like a B-Tree), it buffers all writes in memory, then periodically flushes them to disk as sorted, immutable files. Over time, background processes merge these files together to keep reads efficient.

### The Core Idea (Analogy)
Imagine you're a librarian. Instead of walking to the correct shelf to put each new book away immediately (slow — you're walking back and forth), you:
1. Stack new books on your desk in sorted order (fast — no walking)
2. When your desk is full, carry the whole sorted stack to a "staging shelf" (one trip)
3. Periodically, merge staging shelves together into the main shelves (background work)

This is exactly what an LSM-Tree does:
- **Desk** = Memtable (in-memory sorted structure)
- **Staging shelf** = Level 0 SSTables
- **Main shelves** = Level 1, 2, 3... SSTables
- **Merging** = Compaction

### How It Works Step-by-Step

**Write Path:**
```
Client calls Put("user:123", "Alice")
  → Step 1: Append to WAL on disk (for crash recovery)
  → Step 2: Insert into Memtable (in-memory skip list)
  → Step 3: Return success to client

When Memtable reaches 4MB:
  → Step 4: Freeze the Memtable (make it read-only)
  → Step 5: Create a new empty Memtable for future writes
  → Step 6: Background thread writes frozen Memtable to disk as an SSTable file
```

**Read Path:**
```
Client calls Get("user:123")
  → Step 1: Check active Memtable (newest data)
  → Step 2: Check frozen Memtables (still in memory, awaiting flush)
  → Step 3: Check Level 0 SSTables (ALL of them — they may overlap)
  → Step 4: Check Level 1 SSTables (at most ONE — non-overlapping)
  → Step 5: Check Level 2, 3, 4... (at most one per level)
  → Return first match found, or "not found"
```

The key insight: **we always check newest data first**. If key "X" was written, then deleted, then written again — we find the latest version immediately.

### Why Not a B-Tree?
| | LSM-Tree (our choice) | B-Tree (e.g., PostgreSQL, MySQL InnoDB) |
|---|---|---|
| **Write** | Sequential I/O (append to log, flush sorted batch) | Random I/O (update page in-place) |
| **Read** | May check multiple levels | Single tree traversal |
| **Best for** | Write-heavy workloads | Read-heavy workloads |
| **Used by** | LevelDB, RocksDB, Cassandra, HBase | PostgreSQL, MySQL, SQLite |

The PDF requirement says "high throughput, especially when writing an incoming stream of random items" — this is exactly the LSM-Tree's strength.

### Where in our code
- `engine/engine.go` — orchestrates the entire LSM-Tree (Put, Get, ReadRange)
- Write path: lines 171-183 (Put), 418-444 (maybeScheduleFlush)
- Read path: lines 237-299 (Get)
- Background flush: lines 447-541 (flushLoop, flushMemtable)

### Reading Material
- **Original Paper:** [The Log-Structured Merge-Tree (O'Neil et al., 1996)](https://www.cs.umb.edu/~poneil/lsmtree.pdf) — the academic foundation
- **Practical Guide:** [LSM-Tree by Ben Stopford](https://www.benstopford.com/2015/02/14/log-structured-merge-trees/) — excellent visual walkthrough
- **RocksDB Wiki:** [RocksDB Leveled Compaction](https://github.com/facebook/rocksdb/wiki/Leveled-Compaction) — production implementation details
- **Designing Data-Intensive Applications (Kleppmann), Chapter 3** — "Storage and Retrieval" covers LSM-Trees vs B-Trees in depth

---

## 2. Write-Ahead Log (WAL)

### What is it?
A WAL is a sequential, append-only file where every database mutation is written BEFORE the actual data is modified. If the system crashes, the WAL can be replayed to recover all committed operations.

### Why Do We Need It? (The Problem)
Our Memtable lives in RAM. RAM is volatile — if the process crashes or power is lost, everything in RAM disappears. So after a user calls `Put("key", "value")` and we return "success," we need a guarantee that this write won't be lost.

The WAL solves this: before we even touch the Memtable, we append the operation to a file on disk. If we crash, we read this file on startup and replay all operations.

### How It Works Step-by-Step

```
Put("hello", "world"):

1. Encode the record:
   [CRC32: 4 bytes][DataLen: 4 bytes][Type: 1 byte][KeyLen: 4 bytes]["hello"][ValueLen: 4 bytes]["world"]

2. Write to buffered writer (in memory)
3. Flush buffer to OS (bufio.Flush)
4. Force OS to write to physical disk (file.Sync / fsync)
5. NOW insert into Memtable

On crash recovery:
1. Open all WAL files in order (000001.wal, 000002.wal, ...)
2. For each file, read records sequentially
3. Verify CRC32 of each record
4. If CRC matches: replay into fresh Memtable
5. If CRC fails: stop reading (tail corruption from crash mid-write)
```

### The Critical Concept: fsync
This is the most important thing to understand about the WAL.

When you call `file.Write()`, the data goes to the **OS kernel's buffer cache**, NOT to the physical disk. The OS decides when to actually write it (could be seconds later). If power is lost before the OS writes, the data is gone.

`file.Sync()` (which calls the `fsync` system call) forces the OS to flush its buffer to the physical disk surface. Only after fsync returns can we guarantee the data survives a power failure.

**This is why single writes are ~250 ops/sec** — each fsync takes ~3-4ms on an SSD. It's the price of durability.

**BatchPut optimization:** Instead of fsync per write, we write 100 records then fsync once. This gives us ~25,000 ops/sec — a 100x improvement by amortizing the fsync cost.

### CRC32 Checksums
**What:** A 32-bit Cyclic Redundancy Check computed over the record data.
**Why:** Detects corruption. If we crash mid-write, the last record in the WAL might be partially written (e.g., key was written but value was cut off). The CRC won't match, so we know to discard that record.
**How:** `hash/crc32.ChecksumIEEE(data)` — a well-known polynomial-based error detection code. It's NOT a cryptographic hash (not collision-resistant), but excellent for detecting accidental corruption.

### Where in our code
- `wal/wal.go` — entire implementation
- Append with fsync: lines 104-146
- Batch append: lines 148-157
- Replay with corruption tolerance: lines 184-248
- Record encoding: lines 330-362

### Reading Material
- **PostgreSQL WAL docs:** [Write-Ahead Logging](https://www.postgresql.org/docs/current/wal-intro.html) — how a production database does it
- **"fsync" and durability:** [Don't Trust fsync (LWN.net)](https://lwn.net/Articles/752063/) — deep dive into the subtleties
- **Designing Data-Intensive Applications, Chapter 3** — covers WAL in the context of storage engines

---

## 3. Skip List

### What is it?
A skip list is a probabilistic data structure that provides O(log n) search, insertion, and deletion — the same as a balanced binary search tree — but with a much simpler implementation.

### How It Works (Visual)

A regular linked list (Level 0) has O(n) search — you scan every element:
```
Level 0: 1 → 3 → 5 → 7 → 9 → 11 → 13 → 15 → nil
```

A skip list adds "express lanes" above Level 0:
```
Level 3: 1 ──────────────────────────────────→ 15 → nil
Level 2: 1 ──────────→ 7 ──────────→ 13 ────→ 15 → nil
Level 1: 1 ────→ 5 ──→ 7 ────→ 11 → 13 ────→ 15 → nil
Level 0: 1 → 3 → 5 → 7 → 9 → 11 → 13 → 15 → nil
```

**Searching for 11:**
1. Start at Level 3: 1 → 15 (too far) → drop down
2. Level 2: 1 → 7 → 13 (too far) → drop down
3. Level 1: 7 → 11 → FOUND!

Only 4 comparisons instead of 6 (linear scan).

### The Probabilistic Part
When inserting a new node, we flip a "biased coin" (25% heads) to decide its height:
- Level 0: always (100%)
- Level 1: 25% chance
- Level 2: 25% × 25% = 6.25% chance
- Level 3: 25%³ = 1.5% chance
- ...

This random promotion creates the "express lane" structure WITHOUT needing complex rebalancing (like AVL or Red-Black trees).

**Expected height:** log₄(n). For 1 million keys: ~10 levels. For 1 trillion keys: ~20 levels (our maxLevel).

### Time Complexity
| Operation | Average | Worst Case |
|-----------|---------|------------|
| Search | O(log n) | O(n) (extremely unlikely) |
| Insert | O(log n) | O(n) |
| Delete | O(log n) | O(n) |
| Iteration | O(1) per Next() | O(1) |

### Why Skip List Over Red-Black Tree?
1. **Simpler code:** ~300 lines vs ~600+ for a correct RB-tree
2. **Range iteration is natural:** just follow Level 0 links (no in-order traversal complexity)
3. **Lock-free versions exist:** for future concurrent improvements
4. **Same asymptotic complexity:** both O(log n)

### Tombstones (How Deletes Work)
We don't physically remove entries from the skip list. Instead, `Delete("key")` inserts a **tombstone** — a special marker (nil value) that means "this key was deleted." When reading, if we find a tombstone, we treat it as "not found."

Why? Because the skip list data eventually flushes to immutable SSTables on disk. We need the tombstone to "shadow" older versions of the key in lower levels. Without it, a Get would fall through to an older SSTable and find the stale value.

Tombstones are eventually cleaned up during compaction at the deepest level.

### Where in our code
- `memtable/skiplist.go` — full skip list implementation
- Random level generation: lines 49-56
- Put (insert/update): lines 60-105
- Get (search): lines 109-123
- Delete (tombstone): lines 126-128
- Iterator: lines 140-215

### Reading Material
- **Original Paper:** [Skip Lists: A Probabilistic Alternative to Balanced Trees (Pugh, 1990)](https://15721.courses.cs.cmu.edu/spring2018/papers/08-oltpindexes1/pugh-skiplists-cacm1990.pdf) — the foundational paper, very readable
- **Visual Explanation:** [Skip List Visualization (USFCA)](https://www.cs.usfca.edu/~galles/visualization/SkipList.html) — interactive animation
- **Wikipedia:** [Skip List](https://en.wikipedia.org/wiki/Skip_list) — good overview with pseudocode

---

## 4. SSTable (Sorted String Table)

### What is it?
An SSTable is an immutable file on disk containing key-value pairs sorted by key. Once written, it's never modified — only read or deleted entirely.

### Why "Sorted"?
Sorting enables:
1. **Binary search** — find any key in O(log n) instead of O(n)
2. **Efficient range queries** — keys in [start, end] are contiguous on disk
3. **Efficient merging** — merging two sorted files is O(n), not O(n²)

### The On-Disk Format (Our Design)

```
┌─────────────────────────────────────┐
│         Data Block 0                │  ← ~4KB of sorted KV pairs
├─────────────────────────────────────┤
│         Data Block 1                │
├─────────────────────────────────────┤
│         ...                         │
├─────────────────────────────────────┤
│         Data Block N                │
├─────────────────────────────────────┤
│         Index Block                 │  ← maps last_key → block_offset
├─────────────────────────────────────┤
│         Bloom Filter Block          │  ← probabilistic membership test
├─────────────────────────────────────┤
│         Footer (48 bytes)           │  ← offsets to index + bloom
└─────────────────────────────────────┘
```

**Data Block Entry:**
```
[KeyLen: 4 bytes][ValueLen: 4 bytes][Type: 1 byte][Key: variable][Value: variable]
Type: 0x01 = regular value, 0x02 = tombstone (deletion marker)
```

**Each data block ends with:**
```
[NumEntries: 4 bytes][CRC32: 4 bytes]
```

### Why 4KB Blocks?
- Matches the OS page size (most OSes use 4KB pages)
- Matches SSD internal page size
- When we mmap the file, each block aligns with one virtual memory page
- Reading one key reads at most one 4KB page from disk

### How a Get() Works on an SSTable
```
Get("user:500"):

1. Check Bloom Filter: MayContain("user:500")?
   → false: SKIP this SSTable entirely (no I/O!)
   → true: continue...

2. Binary search the Index Block:
   - Index entries: [("user:100", offset=0), ("user:300", offset=4096), ("user:700", offset=8192)]
   - "user:500" > "user:300" and < "user:700" → it's in block at offset 4096

3. Read Data Block at offset 4096 (from mmap, no syscall)

4. Linear scan within the block:
   - "user:301"... "user:422"... "user:500" → FOUND!
```

### Where in our code
- `sstable/writer.go` — produces SSTable files
- `sstable/reader.go` — reads via mmap
- `sstable/iterator.go` — sequential scan

### Reading Material
- **Google Bigtable Paper:** [Bigtable: A Distributed Storage System](https://static.googleusercontent.com/media/research.google.com/en//archive/bigtable-osdi06.pdf) — Section 6.1 describes SSTables (Google invented the term)
- **LevelDB SSTable format:** [LevelDB Table Format](https://github.com/google/leveldb/blob/main/doc/table_format.md) — our format is inspired by this

---

## 5. Bloom Filter

### What is it?
A Bloom filter is a space-efficient probabilistic data structure that tells you:
- "This key is **definitely NOT** in the set" (100% certain — no false negatives)
- "This key **MIGHT BE** in the set" (small chance of being wrong — false positives)

### Why Do We Need It?
Without Bloom filters, every `Get("key")` that doesn't exist would need to check EVERY SSTable file on disk. With 100 SSTables, that's 100 disk reads for a key that doesn't exist.

With Bloom filters, we ask each SSTable's filter first. If it says "no," we skip that SSTable entirely. With a 1% false positive rate, we go from 100 disk reads to ~1 disk read.

### How It Works (Step by Step)

**Setup:** A bit array of m bits, initially all 0. k hash functions.

**Adding "hello":**
```
h1("hello") = 3    → set bit 3 to 1
h2("hello") = 7    → set bit 7 to 1
h3("hello") = 12   → set bit 12 to 1

Bit array: [0,0,0,1,0,0,0,1,0,0,0,0,1,0,0,0]
                  ^           ^           ^
```

**Checking "hello":**
```
h1("hello") = 3    → bit 3 is 1 ✓
h2("hello") = 7    → bit 7 is 1 ✓
h3("hello") = 12   → bit 12 is 1 ✓
→ "MIGHT be in the set" (all bits set)
```

**Checking "world":**
```
h1("world") = 3    → bit 3 is 1 ✓
h2("world") = 9    → bit 9 is 0 ✗
→ "DEFINITELY NOT in the set" (at least one bit is 0)
```

### The Math

**False positive probability:**
```
P(false positive) ≈ (1 - e^(-kn/m))^k

Where:
  n = number of keys added
  m = number of bits in the array
  k = number of hash functions
```

**Optimal k (number of hash functions):**
```
k_optimal = (m/n) × ln(2) ≈ 0.693 × (m/n)
```

**Our settings:**
- bitsPerKey = 10 → m = 10n
- k = 10 × 0.693 ≈ 7 hash functions
- P(false positive) ≈ 0.82%

**Space:** Only 10 bits (1.25 bytes) per key. For 1 million keys: ~1.25 MB. Incredibly compact.

### Double Hashing Trick
Instead of implementing 7 independent hash functions, we use the **Kirsch-Mitzenmacker technique:**
```
h_i(key) = h1(key) + i × h2(key)    for i = 0, 1, ..., k-1

Where:
  h1 = FNV-32a hash of key
  h2 = upper 32 bits of FNV-64a hash of key
```
This is mathematically proven to give the same false positive rate as k independent hash functions.

### Where in our code
- `sstable/bloom.go`
- Construction: lines 20-30 (NewBloomFilter)
- Add: lines 44-50
- Query: lines 54-62
- Double hashing: lines 94-105

### Reading Material
- **Original Paper:** [Space/Time Trade-offs in Hash Coding with Allowable Errors (Bloom, 1970)](https://dl.acm.org/doi/10.1145/362686.362692)
- **Excellent Visual Explanation:** [Bloom Filters by Example](https://llimllib.github.io/bloomfilter-tutorial/)
- **The double hashing paper:** [Less Hashing, Same Performance (Kirsch & Mitzenmacker, 2006)](https://www.eecs.harvard.edu/~michaelm/postscripts/rsa2008.pdf)
- **Interactive Demo:** [Bloom Filter Calculator](https://hur.st/bloomfilter/)

---

## 6. Memory-Mapped I/O (mmap)

### What is it?
`mmap` maps a file on disk directly into the process's virtual address space. After mmap, you can read file contents by simply accessing a byte slice — the OS kernel loads pages from disk transparently.

### The Normal Way (read/pread)
```
// Traditional file reading:
buf := make([]byte, 4096)      // 1. Allocate userspace buffer (Go heap)
file.ReadAt(buf, offset)        // 2. Syscall → kernel copies data from page cache to buf
// Now data exists in TWO places: kernel page cache AND your Go buffer
```

### The mmap Way
```
// Memory-mapped reading:
data, _ := syscall.Mmap(fd, 0, fileSize, PROT_READ, MAP_PRIVATE)
// data[offset:offset+4096] directly references the kernel page cache
// No copy, no allocation, no syscall per read
```

### Why mmap for Our SSTable Reader?
1. **Zero-copy:** Data blocks are read directly from the kernel's page cache. No `read()` syscall, no buffer allocation, no copying.
2. **Handles datasets larger than RAM:** The OS manages paging. If you mmap a 100GB file on a machine with 8GB RAM, the OS keeps hot pages in RAM and evicts cold pages transparently.
3. **Reduced GC pressure:** The mmap'd byte slice is outside Go's garbage collector. No GC pauses from large heap allocations.
4. **Simplified code:** `data[offset:offset+size]` is a simple slice operation instead of `file.ReadAt(buf, offset)`.

### The Virtual Memory Connection
When you mmap a file:
1. The OS creates virtual memory page table entries for the file
2. Initially, no physical RAM is used (pages are "not present")
3. When you access `data[0]`, a **page fault** occurs
4. The OS loads that 4KB page from disk into RAM (the "page cache")
5. Subsequent accesses to the same page are instant (RAM speed)
6. If RAM is full, the OS evicts the least-recently-used page

This is how we handle "datasets much larger than RAM" — the OS is doing all the work.

### Where in our code
- `sstable/reader.go` line 54 — `syscall.Mmap()`
- Close/cleanup: `syscall.Munmap()` on reader close

### Reading Material
- **Linux mmap man page:** `man mmap` — the authoritative reference
- **LWN.net:** [The mmap() system call](https://lwn.net/Articles/383162/) — deep dive
- **Designing Data-Intensive Applications, Chapter 3** — discusses mmap in context of storage engines

---

## 7. Leveled Compaction

### What is it?
Compaction is the background process that merges SSTable files to: remove deleted/overwritten data, maintain sorted order, and keep read performance predictable.

### Why Is It Needed?
Without compaction:
- Deleted keys still take up disk space (tombstones never cleaned up)
- Overwritten keys have multiple versions across files
- L0 accumulates unlimited overlapping files → Get() gets slower and slower
- Disk usage grows without bound

### Leveled Compaction Strategy

**Level structure:**
```
L0:  [SST-A] [SST-B] [SST-C] [SST-D]    ← up to 4 files, OVERLAPPING keys
     ↓ compact when >= 4 files
L1:  [SST-1] [SST-2] [SST-3]              ← target 10MB, NON-OVERLAPPING
     ↓ compact when > 10MB
L2:  [SST-4] [SST-5] [SST-6] [SST-7]     ← target 100MB, NON-OVERLAPPING
     ↓ compact when > 100MB
L3:  [SST-8] ... [SST-15]                  ← target 1GB
```

**L0 → L1 compaction:**
1. Take ALL L0 files (they may overlap)
2. Find all L1 files whose key range overlaps with the L0 files
3. Merge all of them together using a k-way merge iterator
4. Write new L1 files (~2MB each, non-overlapping)
5. Delete old L0 and L1 files

**L1 → L2 compaction (and so on):**
1. Pick ONE file from L1 (the oldest or most-overlapping)
2. Find all L2 files whose key range overlaps
3. Merge them together
4. Write new L2 files
5. Delete old files

### Write Amplification
**Definition:** Total bytes written to disk ÷ bytes written by user.

```
User writes 1 byte → goes into Memtable → flushed to L0 (1 write)
L0 → L1 compaction: that byte is read + rewritten (1 write)
L1 → L2 compaction: read + rewritten (1 write)
L2 → L3 compaction: read + rewritten (1 write)

Total: 4 disk writes for 1 user write = 4x write amplification
```

With a 10x level ratio, worst case is ~10x per level transition. This is the fundamental trade-off of LSM-Trees: excellent write throughput at the cost of background I/O.

### Where in our code
- `engine/compaction.go` — entire compaction implementation
- L0 compaction: lines 81-183
- Level-to-level: lines 185-261
- Level sizing: lines 12-13 (L1=10MB, file target=2MB)

### Reading Material
- **RocksDB Compaction:** [Leveled Compaction](https://github.com/facebook/rocksdb/wiki/Leveled-Compaction) — production details
- **Size-Tiered vs Leveled:** [Compaction Strategies (ScyllaDB)](https://www.scylladb.com/2018/01/31/compaction-series-space-amplification/) — comparison of strategies
- **Designing Data-Intensive Applications, Chapter 3** — covers compaction in depth

---

## 8. K-Way Merge (Merge Iterator)

### What is it?
An algorithm that takes k sorted sequences and produces one combined sorted sequence. Uses a min-heap (priority queue) for efficiency.

### The Problem
During a range query or compaction, we need to combine data from multiple sources (memtable + multiple SSTables), each of which is independently sorted. We need to produce a single sorted stream.

### How It Works

```
Source A (Memtable): [apple, cherry, grape]
Source B (SSTable 1): [banana, cherry, fig]  ← cherry is a duplicate!
Source C (SSTable 2): [avocado, date, elderberry]

Min-Heap (initially): [(apple,A), (avocado,C), (banana,B)]

Step 1: Pop "apple" from A → output "apple"
         Push A.next="cherry" → heap: [(avocado,C), (banana,B), (cherry,A)]

Step 2: Pop "avocado" from C → output "avocado"
         Push C.next="date" → heap: [(banana,B), (cherry,A), (date,C)]

Step 3: Pop "banana" from B → output "banana"
         Push B.next="cherry" → heap: [(cherry,A), (cherry,B), (date,C)]

Step 4: Pop "cherry" from A (priority 0, newer!)
         Also pop "cherry" from B (same key, skip — older version)
         → output "cherry" (from A only)
         Push A.next="grape", B.next="fig"

...and so on
```

### Priority-Based Deduplication
When two sources have the same key, we need to pick the **newest version**. Each source gets a priority number (lower = newer):
- Active memtable: priority 0
- Immutable memtable: priority 1
- Newest L0 SSTable: priority 2
- Oldest L0 SSTable: priority 3
- L1 SSTables: priority 4, 5, ...

When the heap has two entries with the same key, the one with lower priority (newer) wins. The other is silently skipped.

### Complexity
- **Time:** O(N log k) for the full merge, where N = total entries, k = number of sources
- **Space:** O(k) for the heap
- **Per Next() call:** O(log k) for one heap pop + push

### Where in our code
- `engine/merge_iterator.go`
- Heap implementation: uses `container/heap` from stdlib
- Deduplication logic: lines 118-137 (advance method)

### Reading Material
- **Algorithm textbooks:** Any coverage of "merge k sorted lists" (common interview problem)
- **Go container/heap:** [Go Docs](https://pkg.go.dev/container/heap) — the stdlib heap interface

---

## 9. CRC32 (Cyclic Redundancy Check)

### What is it?
A 32-bit checksum computed over data to detect accidental corruption. Used in our WAL records, SSTable blocks, and manifest records.

### How It Works (Simplified)
CRC treats the data as a very long binary number and divides it by a fixed polynomial. The remainder is the checksum. Any change to the data (even a single flipped bit) produces a completely different remainder.

### Why Not a Cryptographic Hash?
- CRC32 is ~100x faster than SHA-256
- We only need to detect accidental corruption (bit flips, partial writes), not malicious tampering
- CRC32 catches all single-bit errors and virtually all multi-bit errors

### Where We Use It
- WAL: each record has a CRC32 over its payload → detects crash-corrupted tail records
- SSTable: each data block and index block has a CRC32 → detects disk corruption
- Manifest: each record has a CRC32 → detects partial writes

### Where in our code
- `hash/crc32.ChecksumIEEE(data)` used throughout wal.go, sstable/writer.go, manifest.go

---

# PART 2: CONSENSUS & NETWORKING CONCEPTS

---

## 10. Raft Consensus Protocol

### What is it?
Raft is a consensus protocol that allows a cluster of servers to agree on a sequence of operations, even if some servers crash. It's used to replicate our KV store across multiple nodes so that data survives server failures.

### The Problem Raft Solves
If you have one server and it dies, all data is lost. If you have three servers, you need them to agree on the order of operations — otherwise they'll have different data. Raft ensures all servers apply the same operations in the same order.

### The Three Sub-Problems

**1. Leader Election**
- Only ONE node is the leader at any time
- The leader accepts all client writes
- If the leader crashes, a new one is elected

How it works:
```
Time 0: All 3 nodes start as Followers
         Each has a random election timeout (150-300ms)

Time 200ms: Node B's timeout fires first
            Node B becomes a Candidate
            Node B increments its term to 1
            Node B votes for itself
            Node B sends RequestVote to A and C

Time 201ms: Node A receives RequestVote(term=1)
            A hasn't voted in term 1 → grants vote
            Node C receives RequestVote(term=1)
            C hasn't voted in term 1 → grants vote

Time 202ms: Node B has 3 votes (self + A + C) = majority of 3
            Node B becomes Leader
            Node B starts sending heartbeats every 50ms
```

**2. Log Replication**
```
Client: Put("key", "value") → sends to Leader (Node B)

Node B (Leader):
  1. Appends entry to its own log: {Term:1, Index:1, Cmd: Put("key","value")}
  2. Sends AppendEntries RPC to A and C

Node A: receives entry, appends to log, replies SUCCESS
Node C: receives entry, appends to log, replies SUCCESS

Node B: 3/3 nodes have the entry (majority = 2 needed)
  → Entry is COMMITTED
  → Apply to state machine (engine.Put)
  → Reply to client: SUCCESS

Node B sends next heartbeat with LeaderCommit=1
Node A, C: see commitIndex advanced → apply entry to their engines
```

**3. Safety (Election Restriction)**
A voter rejects a candidate whose log is less "up-to-date." This ensures the leader always has all committed entries. "Up-to-date" means: higher last log term, or same term but equal/longer log.

### The Two RPCs

**RequestVote:**
```
Request:  (term, candidateID, lastLogIndex, lastLogTerm)
Response: (term, voteGranted)

Rules:
- Reject if candidate's term < my term
- Grant vote if: I haven't voted in this term AND candidate's log is >= mine
```

**AppendEntries:**
```
Request:  (term, leaderID, prevLogIndex, prevLogTerm, entries[], leaderCommit)
Response: (term, success)

Rules:
- Reject if leader's term < my term
- Reject if my log doesn't match at prevLogIndex/prevLogTerm (consistency check)
- Append entries, update commitIndex
- Empty entries[] = heartbeat
```

### Automatic Failover
```
Time 0:     Leader (A), Follower (B), Follower (C)
Time 100ms: Leader A crashes!
Time 200ms: B and C stop receiving heartbeats
Time 500ms: B's election timer fires (random 300-500ms after last heartbeat)
            B becomes Candidate, requests votes
Time 501ms: C grants vote to B
Time 502ms: B is now Leader (2/3 majority, A is down)
            B starts accepting writes
            Total failover time: ~400ms
```

### Where in our code
- `raft/raft.go` — state machine (election, replication, commit)
- `raft/transport.go` — RPC layer using `net/rpc`
- `raft/log.go` — persistent log storage
- `raft/node.go` — integrates Raft with the KV engine

### Reading Material
- **The Raft Paper:** [In Search of an Understandable Consensus Algorithm (Ongaro & Ousterhout, 2014)](https://web.stanford.edu/~ouster/cgi-bin/papers/raft-atc14.pdf) — THE primary source. Figure 2 is essentially the specification.
- **Interactive Visualization:** [The Secret Lives of Data: Raft](http://thesecretlivesofdata.com/raft/) — EXCELLENT animated walkthrough
- **Raft Website:** [raft.github.io](https://raft.github.io/) — links to all implementations and resources
- **Paxos comparison:** [Paxos Made Simple (Lamport)](https://lamport.azurewebsites.net/pubs/paxos-simple.pdf) — understand what Raft replaced

---

## 11. Binary Wire Protocol

### What is it?
A custom TCP protocol for communication between clients and the KV store server. Uses binary encoding (not text like HTTP) for minimum overhead.

### Why Not HTTP?
HTTP adds ~500 bytes of headers per request (Host, Content-Type, Accept, etc.). Our binary protocol adds only 5 bytes of overhead (4-byte length + 1-byte command). For a KV store where operations are tiny (Put a 10-byte key), HTTP overhead can exceed the actual data.

### Frame Format
```
Request:  [Length: 4 bytes][Command: 1 byte][Payload: variable]
Response: [Length: 4 bytes][Status: 1 byte][Payload: variable]
```

**Length-prefix framing** solves TCP's "stream" problem: TCP delivers bytes, not messages. The receiver reads 4 bytes to learn how long the message is, then reads exactly that many more bytes.

### Where in our code
- `server/protocol.go` — encoding/decoding
- `server/tcp.go` — TCP server with goroutine-per-connection
- `client/client.go` — client library speaking the protocol

---

## 12. Server-Sent Events (SSE)

### What is it?
A simple HTTP-based protocol for the server to push events to the browser in real-time. Unlike WebSockets, SSE is one-directional (server → client only) and uses regular HTTP.

### How It Works
```
Client: GET /api/cluster/events
Server: HTTP/1.1 200 OK
        Content-Type: text/event-stream
        Cache-Control: no-cache

        data: {"type":"put","data":"key=hello"}\n\n
        data: {"type":"leader_elected","data":"node-2"}\n\n
        ...keeps connection open, sends more events...
```

The browser's `EventSource` API handles reconnection automatically.

### Where in our code
- `server/api.go` — SSE handler (handleClusterEvents)
- `server/events.go` — EventBus pub/sub
- Frontend: `new EventSource('/api/cluster/events')` in the dashboard

---

# PART 3: GO PROGRAMMING CONCEPTS

---

## 13. Goroutines & Concurrency

### Goroutine-per-Connection
```go
for {
    conn, _ := listener.Accept()
    go handleConn(conn)  // each connection gets its own goroutine
}
```
Goroutines are ~2-4KB of stack (vs ~1MB for OS threads). We can have thousands of concurrent connections without running out of memory.

### RWMutex (Read-Write Lock)
```go
var mu sync.RWMutex

// Multiple goroutines can read simultaneously:
mu.RLock()
value := data[key]
mu.RUnlock()

// Only one goroutine can write (exclusive):
mu.Lock()
data[key] = newValue
mu.Unlock()
```
We use this in the engine: many concurrent reads (Get), exclusive writes (Put). Reads don't block each other.

### Channels for Signaling
```go
closeCh := make(chan struct{})

// Worker goroutine:
select {
case <-closeCh:
    return  // shutdown signal received
case task := <-taskCh:
    process(task)
}

// To shutdown:
close(closeCh)  // broadcasts to ALL receivers
```

### WaitGroup for Graceful Shutdown
```go
var wg sync.WaitGroup

wg.Add(1)
go func() {
    defer wg.Done()
    // do work...
}()

wg.Wait()  // blocks until all goroutines call Done()
```

---

## 14. Go embed Directive

```go
//go:embed dashboard
var dashboardFS embed.FS
```
This embeds the entire `dashboard/` directory into the compiled binary at build time. The binary is self-contained — no external files needed to serve the web dashboard.

---

## 15. The `container/heap` Interface

Go's stdlib provides a heap (priority queue) via an interface:
```go
type Interface interface {
    Len() int
    Less(i, j int) bool
    Swap(i, j int)
    Push(x interface{})
    Pop() interface{}
}
```
We implement this interface for our merge iterator's min-heap, ordered by (key, priority).

---

# PART 4: POTENTIAL INTERVIEW QUESTIONS & LIVE CHANGES

*(Kept from previous version — see sections 3.1 through 3.4 for architecture deep-dives, Raft questions, performance questions, and 8 likely live code change requests with file-level implementation guidance)*

### Quick Reference: Questions You MUST Be Able to Answer

1. **"Walk me through a Put operation end to end"** — WAL append → fsync → memtable insert → maybe flush → return
2. **"Walk me through a Get operation"** — memtable → immutables → L0 (all, bloom) → L1+ (one per level, bloom → index → block)
3. **"What happens when the leader crashes?"** — election timeout → new election → new leader in <1s → no committed data lost
4. **"How do you handle datasets larger than RAM?"** — mmap SSTables, OS manages page cache
5. **"What's your write amplification?"** — ~10x per level with 10x ratio, 4-5 levels typical = 40-50x worst case
6. **"Why fsync on every write?"** — durability guarantee, price of "not losing data"
7. **"What's the bloom filter false positive rate?"** — ~0.82% with 10 bits/key and 7 hashes
8. **"Why skip list over red-black tree?"** — simpler, same O(log n), better for iteration and concurrency
9. **"What are tombstones?"** — deletion markers that shadow older versions in lower levels, cleaned up during compaction
10. **"What's the election restriction in Raft?"** — candidates must have all committed entries to win, verified by voters checking lastLogTerm and lastLogIndex
