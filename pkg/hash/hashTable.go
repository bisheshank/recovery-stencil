package hash

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"

	"dinodb/pkg/entry"
	"dinodb/pkg/pager"
)

// A HashTable is a database index that uses extendible hashing for quick lookups
type HashTable struct {
	globalDepth int64        // The **global** depth of the Hash Table
	buckets     []int64      // Slice of bucket's page numbers. The indices (in binary) correspond to buckets' search keys in the HashTable
	pager       *pager.Pager // The pager associated with the Hash Table
	rwlock      sync.RWMutex // Lock on the Hash Table
}

// Returns a new HashTable.
func NewHashTable(pager *pager.Pager) (*HashTable, error) {
	depth := int64(2)
	buckets := make([]int64, powInt(2, depth))
	for i := range buckets {
		bucket, err := newHashBucket(pager, depth)
		if err != nil {
			return nil, err
		}
		buckets[i] = bucket.page.GetPageNum()
		pager.PutPage(bucket.page)
	}
	return &HashTable{globalDepth: depth, buckets: buckets, pager: pager}, nil
}

// Get depth.
func (table *HashTable) GetDepth() int64 {
	return table.globalDepth
}

// GetBuckets returns a slice containing the page numbers for all of this table's bucket.
func (table *HashTable) GetBuckets() []int64 {
	return table.buckets
}

// Get pager.
func (table *HashTable) GetPager() *pager.Pager {
	return table.pager
}

// Finds the entry with the given key.
func (table *HashTable) Find(key int64) (entry.Entry, error) {
	table.RLock()
	// Hash the key.
	hash := Hasher(key, table.globalDepth)
	if hash < 0 || int(hash) >= len(table.buckets) {
		table.RUnlock()
		return entry.Entry{}, errors.New("not found")
	}
	// Get the corresponding bucket.
	// [CONCURRENCY]: Using GetAndLockBucket instead of GetBucket
	bucket, err := table.GetAndLockBucket(hash, READ_LOCK)
	if err != nil {
		table.RUnlock()
		return entry.Entry{}, err
	}
	table.RUnlock()
	defer table.pager.PutPage(bucket.page)
	defer bucket.RUnlock()

	// Find the entry.
	foundEntry, found := bucket.Find(key)
	if !found {
		return entry.Entry{}, errors.New("not found")
	}
	return foundEntry, nil
}

// ExtendTable increases the global depth of the table by 1.
func (table *HashTable) ExtendTable() {
	table.globalDepth = table.globalDepth + 1
	table.buckets = append(table.buckets, table.buckets...)
}

// Insert a key / value pair into the Hash Table.
// Make sure to lock both table and buckets
func (table *HashTable) Insert(key int64, value int64) error {
	/* SOLUTION {{{ */
	table.WLock()
	defer table.WUnlock()
	hash := Hasher(key, table.globalDepth)
	// [CONCURRENCY]: Using GetAndLockBucket instead of GetBucket
	bucket, err := table.GetAndLockBucket(hash, WRITE_LOCK)
	if err != nil {
		return err
	}
	defer table.pager.PutPage(bucket.page)
	defer bucket.WUnlock()
	split := bucket.Insert(key, value)
	if !split {
		return nil
	}
	return table.split(bucket, hash)
	/* SOLUTION }}} */
}

// Split the given bucket into two, extending the table if necessary.
//
// It is possible that after rehashing and redistributing, one of the buckets is empty
// and the other one still overflows, immediately requiring a second split.
// This may be a consequence of a bad hash function, but is a possible scenario
// that we should handle.
func (table *HashTable) split(bucket *HashBucket, hash int64) error {
	/* SOLUTION {{{ */
	// Figure out where the new pointer should live.
	oldHash := (hash % powInt(2, bucket.localDepth))
	newHash := oldHash + powInt(2, bucket.localDepth)
	// If we are splitting, check if we need to double the table first.
	if bucket.localDepth == table.globalDepth {
		table.ExtendTable()
	}
	// Next, make a new bucket
	bucket.updateLocalDepth(bucket.localDepth + 1)
	newBucket, err := newHashBucket(table.pager, bucket.localDepth)
	if err != nil {
		return err
	}
	defer table.pager.PutPage(newBucket.page)

	// Move entries over to it.
	tmpEntries := make([]entry.Entry, bucket.numKeys)
	for i := int64(0); i < bucket.numKeys; i++ {
		tmpEntries[i] = bucket.getEntry(i)
	}
	oldNKeys := int64(0)
	newNKeys := int64(0)
	for _, entry := range tmpEntries {
		if Hasher(entry.Key, bucket.localDepth) == newHash {
			newBucket.modifyEntry(newNKeys, entry)
			newNKeys++
		} else {
			bucket.modifyEntry(oldNKeys, entry)
			oldNKeys++
		}
	}
	// Initialize bucket attributes.
	bucket.updateNumKeys(oldNKeys)
	newBucket.updateNumKeys(newNKeys)
	power := bucket.localDepth
	// Point the rest of the buckets to the new page.
	for i := newHash; i < powInt(2, table.globalDepth); i += powInt(2, power) {
		table.buckets[i] = newBucket.page.GetPageNum()
	}
	// Check if recursive splitting is required
	if oldNKeys >= MAX_BUCKET_SIZE {
		return table.split(bucket, oldHash)
	}
	if newNKeys >= MAX_BUCKET_SIZE {
		return table.split(newBucket, newHash)
	}
	return nil
	/* SOLUTION }}} */
}

// Update the given key-value pair.
func (table *HashTable) Update(key int64, value int64) error {
	table.RLock()
	hash := Hasher(key, table.globalDepth)
	// [CONCURRENCY]: Using GetAndLockBucket instead of GetBucket
	bucket, err := table.GetAndLockBucket(hash, WRITE_LOCK)
	if err != nil {
		table.RUnlock()
		return err
	}
	defer table.pager.PutPage(bucket.page)
	table.RUnlock()
	defer bucket.WUnlock()
	err2 := bucket.Update(key, value)
	return err2
}

// Delete the given key-value pair, does not coalesce.
func (table *HashTable) Delete(key int64) error {
	table.RLock()
	hash := Hasher(key, table.globalDepth)
	// [CONCURRENCY]: Using GetAndLockBucket instead of GetBucket
	bucket, err := table.GetAndLockBucket(hash, WRITE_LOCK)
	if err != nil {
		table.RUnlock()
		return err
	}
	defer table.pager.PutPage(bucket.page)
	table.RUnlock()
	defer bucket.WUnlock()
	err2 := bucket.Delete(key)
	return err2
}

// Select all entries in this table.
func (table *HashTable) Select() ([]entry.Entry, error) {
	/* SOLUTION {{{ */
	table.RLock()
	defer table.RUnlock()
	ret := make([]entry.Entry, 0)
	for i := int64(0); i < table.pager.GetNumPages(); i++ {
		bucket, err := table.GetAndLockBucketByPN(i, READ_LOCK)
		if err != nil {
			return nil, err
		}
		entries, err := bucket.Select()
		table.pager.PutPage(bucket.GetPage())
		if err != nil {
			bucket.RUnlock()
			return nil, err
		}
		bucket.RUnlock()
		ret = append(ret, entries...)
	}
	return ret, nil
	/* SOLUTION }}} */
}

// Print writes a string representation of this entire table (including it's buckets) to the specified writer.
func (table *HashTable) Print(w io.Writer) {
	table.RLock()
	defer table.RUnlock()
	io.WriteString(w, "====\n")
	io.WriteString(w, fmt.Sprintf("global depth: %d\n", table.globalDepth))
	for i := range table.buckets {
		io.WriteString(w, fmt.Sprintf("====\nbucket %d\n", i))
		// [CONCURRENCY]: Using GetAndLockBucket instead of GetBucket
		bucket, err := table.GetAndLockBucket(int64(i), READ_LOCK)
		if err != nil {
			continue
		}
		bucket.Print(w)
		bucket.RUnlock()
		table.pager.PutPage(bucket.page)
	}
	io.WriteString(w, "====\n")
}

// Print out a specific bucket.
func (table *HashTable) PrintPN(pn int, w io.Writer) {
	table.RLock()
	defer table.RUnlock()
	if int64(pn) >= table.pager.GetNumPages() {
		fmt.Println("out of bounds")
		return
	}
	bucket, err := table.GetAndLockBucketByPN(int64(pn), READ_LOCK)
	if err != nil {
		return
	}
	bucket.Print(w)
	bucket.RUnlock()
	table.pager.PutPage(bucket.page)
}

// [CONCURRENCY] Grab a write lock on the hash table index
func (table *HashTable) WLock() {
	table.rwlock.Lock()
}

// [CONCURRENCY] Release a write lock on the hash table index
func (table *HashTable) WUnlock() {
	table.rwlock.Unlock()
}

// [CONCURRENCY] Grab a read lock on the hash table index
func (table *HashTable) RLock() {
	table.rwlock.RLock()
}

// [CONCURRENCY] Release a read lock on the hash table index
func (table *HashTable) RUnlock() {
	table.rwlock.RUnlock()
}

/////////////////////////////////////////////////////////////////////////////
////////////////////////// HashTable Helper Functions ///////////////////////
/////////////////////////////////////////////////////////////////////////////

// Returns the bucket in the hash table using its page number, and increments the bucket ref count.
func (table *HashTable) GetBucketByPN(pn int64) (*HashBucket, error) {
	page, err := table.pager.GetPage(pn)
	if err != nil {
		return nil, err
	}
	return pageToBucket(page), nil
}

// Returns the bucket in the hash table using its page number, and increments the bucket ref count.
func (table *HashTable) GetAndLockBucketByPN(pn int64, lock BucketLockType) (*HashBucket, error) {
	page, err := table.pager.GetPage(pn)
	if err != nil {
		return nil, err
	}
	if lock == READ_LOCK {
		page.RLock()
	}
	if lock == WRITE_LOCK {
		page.WLock()
	}
	return pageToBucket(page), nil
}

// Returns the bucket in the hash table, and increments the bucket ref count.
func (table *HashTable) GetBucket(hash int64) (*HashBucket, error) {
	pagenum := table.buckets[hash]
	bucket, err := table.GetBucketByPN(pagenum)
	if err != nil {
		return nil, err
	}
	return bucket, nil
}

// Returns the bucket in the hash table, and increments the bucket ref count.
func (table *HashTable) GetAndLockBucket(hash int64, lock BucketLockType) (*HashBucket, error) {
	pagenum := table.buckets[hash]
	bucket, err := table.GetAndLockBucketByPN(pagenum, lock)
	if err != nil {
		return nil, err
	}
	return bucket, nil
}

// Read hash table in from memory.
func ReadHashTable(bucketPager *pager.Pager) (*HashTable, error) {
	backingFilename := bucketPager.GetFileName() + ".meta"
	indexPager, err := pager.New(backingFilename)
	if err != nil {
		return nil, err
	}
	metaPN := int64(0)
	metaPage, err := indexPager.GetPage(metaPN)
	if err != nil {
		return nil, err
	}
	// Read the gobal depth
	depth, _ := binary.Varint(metaPage.GetData()[:DEPTH_SIZE])
	bytesRead := DEPTH_SIZE
	// Read the bucket index
	pnSize := int64(binary.MaxVarintLen64)
	numHashes := powInt(2, depth)
	buckets := make([]int64, numHashes)
	for i := int64(0); i < numHashes; i++ {
		if bytesRead+pnSize > PAGESIZE {
			indexPager.PutPage(metaPage)
			metaPN++
			metaPage, err = indexPager.GetPage(metaPN)
			if err != nil {
				return nil, err
			}
			bytesRead = 0
		}
		pn, _ := binary.Varint(metaPage.GetData()[bytesRead : bytesRead+pnSize])
		bytesRead += pnSize
		buckets[i] = pn
	}
	indexPager.PutPage(metaPage)
	indexPager.Close()
	return &HashTable{globalDepth: depth, buckets: buckets, pager: bucketPager}, nil
}

// Write hash table out to memory.
func WriteHashTable(bucketPager *pager.Pager, table *HashTable) error {
	backingFilename := bucketPager.GetFileName() + ".meta"
	indexPager, err := pager.New(backingFilename)
	if err != nil {
		return err
	}
	metaPage, err := indexPager.GetNewPage()
	if err != nil {
		return err
	}
	metaPage.SetDirty(true)
	// Write global depth to meta file
	depthData := make([]byte, DEPTH_SIZE)
	binary.PutVarint(depthData, table.globalDepth)
	metaPage.Update(depthData, DEPTH_OFFSET, DEPTH_SIZE)
	bytesWritten := DEPTH_SIZE
	// Write bucket index to meta file
	pnSize := int64(binary.MaxVarintLen64)
	pnData := make([]byte, pnSize)
	for _, pn := range table.buckets {
		if bytesWritten+pnSize > PAGESIZE {
			indexPager.PutPage(metaPage)
			metaPage, err = indexPager.GetNewPage()
			if err != nil {
				return err
			}
			metaPage.SetDirty(true)
			bytesWritten = 0
		}
		binary.PutVarint(pnData, pn)
		metaPage.Update(pnData, bytesWritten, pnSize)
		bytesWritten += pnSize
	}
	indexPager.PutPage(metaPage)
	indexPager.Close()
	return bucketPager.Close()
}

// x^y
func powInt(x, y int64) int64 {
	return int64(math.Pow(float64(x), float64(y)))
}
