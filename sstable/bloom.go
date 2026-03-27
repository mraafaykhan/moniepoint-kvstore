package sstable

import (
	"encoding/binary"
	"errors"
	"hash/fnv"
)

// BloomFilter is a probabilistic data structure for set membership testing.
type BloomFilter struct {
	bits []byte
	m    uint32 // number of bits
	k    uint32 // number of hash functions
}

// NewBloomFilter creates a new Bloom filter sized for numKeys with bitsPerKey bits per key.
// If bitsPerKey <= 0, it defaults to 10.
func NewBloomFilter(numKeys int, bitsPerKey int) *BloomFilter {
	if bitsPerKey <= 0 {
		bitsPerKey = 10
	}
	if numKeys < 1 {
		numKeys = 1
	}
	m := uint32(numKeys * bitsPerKey)
	if m < 8 {
		m = 8
	}
	k := uint32(float64(bitsPerKey) * 0.693)
	if k < 1 {
		k = 1
	}
	if k > 30 {
		k = 30
	}
	numBytes := (m + 7) / 8
	return &BloomFilter{
		bits: make([]byte, numBytes),
		m:    m,
		k:    k,
	}
}

// Add inserts a key into the Bloom filter.
func (bf *BloomFilter) Add(key []byte) {
	h1, h2 := bloomHashes(key)
	for i := uint32(0); i < bf.k; i++ {
		bit := (h1 + i*h2) % bf.m
		bf.bits[bit/8] |= 1 << (bit % 8)
	}
}

// MayContain returns true if the key might be in the set, false if it is definitely not.
func (bf *BloomFilter) MayContain(key []byte) bool {
	h1, h2 := bloomHashes(key)
	for i := uint32(0); i < bf.k; i++ {
		bit := (h1 + i*h2) % bf.m
		if bf.bits[bit/8]&(1<<(bit%8)) == 0 {
			return false
		}
	}
	return true
}

// Encode serializes the Bloom filter to bytes: [m:4][k:4][bits...].
func (bf *BloomFilter) Encode() []byte {
	buf := make([]byte, 8+len(bf.bits))
	binary.LittleEndian.PutUint32(buf[0:4], bf.m)
	binary.LittleEndian.PutUint32(buf[4:8], bf.k)
	copy(buf[8:], bf.bits)
	return buf
}

// DecodeBloomFilter deserializes a Bloom filter from bytes.
func DecodeBloomFilter(data []byte) (*BloomFilter, error) {
	if len(data) < 8 {
		return nil, errors.New("bloom filter data too short")
	}
	m := binary.LittleEndian.Uint32(data[0:4])
	k := binary.LittleEndian.Uint32(data[4:8])
	numBytes := (m + 7) / 8
	if uint32(len(data)-8) < numBytes {
		return nil, errors.New("bloom filter data truncated")
	}
	bits := make([]byte, numBytes)
	copy(bits, data[8:8+numBytes])
	return &BloomFilter{
		bits: bits,
		m:    m,
		k:    k,
	}, nil
}

// bloomHashes computes the two hash values used for double hashing.
func bloomHashes(key []byte) (uint32, uint32) {
	h1 := fnv.New32a()
	h1.Write(key)
	hash1 := h1.Sum32()

	h2 := fnv.New64a()
	h2.Write(key)
	hash2 := uint32(h2.Sum64() >> 32)

	return hash1, hash2
}
