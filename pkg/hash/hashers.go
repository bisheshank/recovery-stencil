package hash

import (
	"encoding/binary"

	"github.com/cespare/xxhash"
	"github.com/spaolacci/murmur3"
)

// getHash uses the given hasher function to calculate and return
// the hash of a key modded by the size.
func getHash(hasher func(b []byte) uint64, key int64, size int64) uint {
	buf := make([]byte, binary.MaxVarintLen64)
	binary.PutVarint(buf, key)
	hash := int64(hasher(buf))
	if hash < 0 {
		hash *= -1
	}
	return uint(hash % size)
}

// XxHasher returns the xxHash hash of the given key, bounded by size.
func XxHasher(key int64, size int64) uint {
	return getHash(xxhash.Sum64, key, size)
}

// MurmurHasher returns the MurmurHash3 hash of the given key, bounded by size.
func MurmurHasher(key int64, size int64) uint {
	return getHash(murmur3.Sum64, key, size)
}

// Hasher returns the hash of a key, modded by 2^depth.
func Hasher(key int64, depth int64) int64 {
	return int64(XxHasher(key, powInt(2, depth)))
}
