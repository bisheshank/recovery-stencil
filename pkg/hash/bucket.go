package hash

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"dinodb/pkg/entry"
	"dinodb/pkg/pager"
)

// HashBucket represents a bucket within a hash table.
type HashBucket struct {
	localDepth int64       // The **local** depth of the Hash Bucket
	numKeys    int64       // The number of keys / entries in the Hash Bucket
	page       *pager.Page // The page containing the bucket's data
}

// [CONCURRENCY] Enumerates 3 different locking operations: no lock, write lock, or read lock.
type BucketLockType int

const (
	NO_LOCK    BucketLockType = 0
	WRITE_LOCK BucketLockType = 1
	READ_LOCK  BucketLockType = 2
)

// newHashBucket constructs a new, empty HashBucket with the specified local depth
// using a new page from the specified pager.
// The new page must be put by the caller of this method.
func newHashBucket(pager *pager.Pager, depth int64) (*HashBucket, error) {
	newPage, err := pager.GetNewPage()
	if err != nil {
		return nil, err
	}
	bucket := &HashBucket{localDepth: depth, numKeys: 0, page: newPage}
	bucket.updateLocalDepth(depth)
	return bucket, nil
}

// GetDepth returns the bucket's local depth.
func (bucket *HashBucket) GetDepth() int64 {
	return bucket.localDepth
}

// Get a bucket's page.
func (bucket *HashBucket) GetPage() *pager.Page {
	return bucket.page
}

// Find returns an entry in the bucket with the given key.
func (bucket *HashBucket) Find(key int64) (entry.Entry, bool) {
	for i := int64(0); i < bucket.numKeys; i++ {
		if bucket.getKeyAt(i) == key {
			return bucket.getEntry(i), true
		}
	}
	return entry.Entry{}, false
}

// Inserts the given key-value pair, allowing duplicate keys.
// Returns whether the bucket needs to split after this insertion.
func (bucket *HashBucket) Insert(key int64, value int64) bool {
	/* SOLUTION {{{ */
	bucket.modifyEntry(bucket.numKeys, entry.New(key, value))
	bucket.updateNumKeys(bucket.numKeys + 1)
	// If we reach the max number of keys a Hash Bucket can store, we must split
	return bucket.numKeys >= MAX_BUCKET_SIZE
	/* SOLUTION }}} */
}

// Update modifies the value associated with a given key, or returns an error
// if no entry with that key is found.
// This method should never split the bucket.
func (bucket *HashBucket) Update(key int64, newValue int64) error {
	// Get the index to update.
	index := int64(-1)
	for i := int64(0); i < bucket.numKeys; i++ {
		if bucket.getKeyAt(i) == key {
			index = i
			break
		}
	}
	if index == -1 {
		return errors.New("key not found, update aborted")
	}
	// Update the value.
	bucket.updateValueAt(index, newValue)
	return nil
}

// Delete deletes the key-value entry with the specified key, or returns an error
// if no entry with that key is found.
// NOTE: does not coalesce (ie doesn't merge buckets when they become empty)
func (bucket *HashBucket) Delete(key int64) error {
	// Get the index to delete.
	index := int64(-1)
	for i := int64(0); i < bucket.numKeys; i++ {
		if bucket.getKeyAt(i) == key {
			index = i
			break
		}
	}
	if index == -1 {
		return errors.New("key not found, delete aborted")
	}
	// Move all other keys left by one.
	for i := index; i < bucket.numKeys-1; i++ {
		bucket.modifyEntry(i, bucket.getEntry(i+1))
	}
	bucket.updateNumKeys(bucket.numKeys - 1)
	return nil
}

// Select returns all key-value entries within this bucket.
func (bucket *HashBucket) Select() ([]entry.Entry, error) {
	ret := make([]entry.Entry, 0)
	for i := int64(0); i < bucket.numKeys; i++ {
		ret = append(ret, bucket.getEntry(i))
	}
	return ret, nil
}

// Print writes a string-representation of this bucket and it's entries to the specified writer.
func (bucket *HashBucket) Print(w io.Writer) {
	io.WriteString(w, fmt.Sprintf("bucket depth: %d\n", bucket.localDepth))
	io.WriteString(w, "entries:")
	for i := int64(0); i < bucket.numKeys; i++ {
		bucket.getEntry(i).Print(w)
	}
	io.WriteString(w, "\n")
}

// [CONCURRENCY] Grab a write lock on the hash table index
func (bucket *HashBucket) WLock() {
	bucket.page.WLock()
}

// [CONCURRENCY] Release a write lock on the hash table index
func (bucket *HashBucket) WUnlock() {
	bucket.page.WUnlock()
}

// [CONCURRENCY] Grab a read lock on the hash table index
func (bucket *HashBucket) RLock() {
	bucket.page.RLock()
}

// [CONCURRENCY] Release a read lock on the hash table index
func (bucket *HashBucket) RUnlock() {
	bucket.page.RUnlock()
}

/////////////////////////////////////////////////////////////////////////////
///////////////////// HashBucket Helper Functions ///////////////////////////
/////////////////////////////////////////////////////////////////////////////

// entryPos gets the byte-position of the entry with the given index.
func entryPos(index int64) int64 {
	return BUCKET_HEADER_SIZE + index*ENTRYSIZE
}

// modifyEntry writes the given entry into the bucket's page at the given index.
func (bucket *HashBucket) modifyEntry(index int64, entry entry.Entry) {
	newdata := entry.Marshal()
	offsetPos := entryPos(index)
	bucket.page.Update(newdata, offsetPos, ENTRYSIZE)
}

// getEntry returns the entry at the given index.
func (bucket *HashBucket) getEntry(index int64) entry.Entry {
	startPos := entryPos(index)
	entry := entry.UnmarshalEntry(bucket.page.GetData()[startPos : startPos+ENTRYSIZE])
	return entry
}

// getKeyAt returns the key at the given index.
func (bucket *HashBucket) getKeyAt(index int64) int64 {
	return bucket.getEntry(index).Key
}

// updateKeyAt updates the key of the entry at the given index.
func (bucket *HashBucket) updateKeyAt(index int64, newKey int64) {
	existingVal := bucket.getValueAt(index)
	bucket.modifyEntry(index, entry.New(newKey, existingVal))
}

// Get the value at the given index.
func (bucket *HashBucket) getValueAt(index int64) int64 {
	return bucket.getEntry(index).Value
}

// updateValueAt updates the value of the entry at the given index.
func (bucket *HashBucket) updateValueAt(index int64, newValue int64) {
	existingKey := bucket.getKeyAt(index)
	bucket.modifyEntry(index, entry.New(existingKey, newValue))
}

// updateDepth updates this bucket's depth and writes the new depth to the bucket's page.
func (bucket *HashBucket) updateLocalDepth(newDepth int64) {
	bucket.localDepth = newDepth
	depthData := make([]byte, DEPTH_SIZE)
	binary.PutVarint(depthData, newDepth)
	bucket.page.Update(depthData, DEPTH_OFFSET, DEPTH_SIZE)
}

// updateNumKeys update number of keys in this bucket, writing the new numKeys to the bucket's page.
func (bucket *HashBucket) updateNumKeys(newNumKeys int64) {
	bucket.numKeys = newNumKeys
	nKeysData := make([]byte, NUM_KEYS_SIZE)
	binary.PutVarint(nKeysData, newNumKeys)
	bucket.page.Update(nKeysData, NUM_KEYS_OFFSET, NUM_KEYS_SIZE)
}

// pageToBucket converts the given page into a HashBucket struct.
func pageToBucket(page *pager.Page) *HashBucket {
	depth, _ := binary.Varint(
		page.GetData()[DEPTH_OFFSET : DEPTH_OFFSET+DEPTH_SIZE],
	)
	numKeys, _ := binary.Varint(
		page.GetData()[NUM_KEYS_OFFSET : NUM_KEYS_OFFSET+NUM_KEYS_SIZE],
	)
	return &HashBucket{
		localDepth: depth,
		numKeys:    numKeys,
		page:       page,
	}
}
