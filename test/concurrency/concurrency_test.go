package concurrency_test

import (
	"fmt"
	"math/rand"
	"slices"
	"testing"
	"time"

	"dinodb/pkg/btree"
	"dinodb/pkg/database"
	"dinodb/pkg/entry"
	"dinodb/pkg/hash"
	"dinodb/test/utils"
)

var BUFFER_SIZE int = 1024
var DELAY_TIME = 10 * time.Millisecond
var MAX_DELAY int64 = 10

var concurrencySalt = utils.Salt

func TestConcurrentIndex(t *testing.T) {
	t.Run("HashInsert", testConcurrentHashInsert)
	t.Run("HashSelect", testConcurrentHashSelect)
	t.Run("BTreeInsert", testConcurrentBTreeInsert)
	t.Run("BTreeSelect", testConcurrentBTreeSelect)
	// t.Run("BTreeInsertAndSelect", testConcurrentBTreeInsertAndSelect)
}

// =====================================================================
// HELPERS
// ====================================================================='

// setupIndex creates and opens an index of the specified type.
// Also defers the closing and deletion of the index
func setupIndex(t *testing.T, indexType database.IndexType) database.Index {
	t.Parallel()
	dbName := utils.GetTempDbFile(t)
  
	var index database.Index
	var err error
	if indexType == database.BTreeIndexType {
		index, err = btree.OpenIndex(dbName)
	} else if indexType == database.HashIndexType {
		index, err = hash.OpenTable(dbName)
	} else {
		panic("Unknown index type")
	}
	if err != nil {
		t.Fatalf("Failed to create %s index: %q", indexType, err)
	}

	utils.EnsureCleanup(t, func() {
		// don't care about close error, just want to cleanup
		_ = index.Close()
	})

	return index
}

func jitter() time.Duration {
	return time.Duration(rand.Int63n(MAX_DELAY)+1) * time.Millisecond
}

func insertKeys(table database.Index, insertCh <-chan int64, doneCh chan<- bool, errCh chan<- error) {
	for v := range insertCh {
		time.Sleep(jitter())
		err := table.Insert(v, v%concurrencySalt)
		if err != nil {
			errCh <- fmt.Errorf("Failed to concurrently insert (%d, %d) into the index: %s", v, v%concurrencySalt, err)
			return
		}
	}
	doneCh <- true
}

func selectKeys(table database.Index, numTimesToSelect int, expectedResults []entry.Entry, done chan<- bool, errCh chan<- error) {
	for range numTimesToSelect {
		time.Sleep(jitter())
		entries, errSelect := table.Select()
		if errSelect != nil {
			errCh <- fmt.Errorf("Concurrent select failed: %s", errSelect)
			return
		}

		if len(entries) != len(expectedResults) {
			errCh <- fmt.Errorf("Concurrent select returned %d entries, but expected %d entries", len(entries), len(expectedResults))
			return
		}

		for _, entry := range expectedResults {
			if !slices.Contains(entries, entry) {
				errCh <- fmt.Errorf("Concurrent select is missing (%d, %d) in it's results", entry.Key, entry.Value)
				return
			}
		}
	}
	done <- true
}

// =====================================================================
// TESTS (Fine-grain Locking)
// =====================================================================

func testConcurrentHashInsert(t *testing.T) {
	index := setupIndex(t, database.HashIndexType)

	// Queue entries for insertion
	nums := make(chan int64, 100)
	inserted := make([]int64, 0)
	target := int64(3)
	targetDepth := int64(4)
	go func() {
		cur := int64(0)
		for i := int64(0); i <= 5000; i++ {
			for {
				cur += 1
				if hash.Hasher(cur, targetDepth) == target {
					nums <- cur
					inserted = append(inserted, cur)
					break
				}
			}
		}
		close(nums)
	}()
	done := make(chan bool)
	errCh := make(chan error)
	numThreads := 4
	for i := 0; i < numThreads; i++ {
		go insertKeys(index, nums, done, errCh)
	}
	for i := 0; i < numThreads; i++ {
		select {
		case <-done:
			continue
		case err := <-errCh:
			t.Fatal(err)
		}
	}
	// Retrieve entries
	for _, i := range inserted {
		entry, err := index.Find(i)
		if err != nil {
			t.Fatal(err)
		}
		if entry.Value != i%concurrencySalt {
			t.Fatal("Entry found has the wrong value")
		}
	}
}

func testConcurrentHashSelect(t *testing.T) {
	index := setupIndex(t, database.HashIndexType)

	numInsertions := int64(5_000)
	allEntries := make([]entry.Entry, numInsertions)
	// Insert 5000 entries (not concurrently)
	for i := range numInsertions {
		utils.InsertEntry(t, index, i, i%concurrencySalt)
		allEntries[i] = entry.New(i, i%concurrencySalt)
	}

	numThreads := 4
	numSelectsPerThread := 200
	// Select all entries several times concurrently
	done := make(chan bool)
	errCh := make(chan error)
	for range numThreads {
		go selectKeys(index, numSelectsPerThread, allEntries, done, errCh)
	}

	for range numThreads {
		select {
		case <-done:
			continue
		case err := <-errCh:
			t.Fatal(err)
		}
	}
}

func testConcurrentBTreeInsert(t *testing.T) {
	index := setupIndex(t, database.BTreeIndexType)

	// Queue entries for insertion
	nums := make(chan int64, 100)
	inserted := make([]int64, 0)
	go func() {
		for i := int64(0); i <= 5000; i++ {
			nums <- i
			inserted = append(inserted, i)
		}
		close(nums)
	}()
	done := make(chan bool)
	errCh := make(chan error)
	numThreads := 4
	for i := 0; i < numThreads; i++ {
		go insertKeys(index, nums, done, errCh)
	}
	for i := 0; i < numThreads; i++ {
		select {
		case <-done:
			continue
		case err := <-errCh:
			t.Fatal(err)
		}
	}
	// Retrieve entries
	for _, i := range inserted {
		entry, err := index.Find(i)
		if err != nil {
			t.Error(err)
		}
		if entry.Value != i%concurrencySalt {
			t.Error("Entry found has the wrong value")
		}
	}
}

func testConcurrentBTreeSelect(t *testing.T) {
	index := setupIndex(t, database.BTreeIndexType)

	numInsertions := int64(5_000)
	allEntries := make([]entry.Entry, numInsertions)
	// Insert 5000 entries (not concurrently)
	for i := range numInsertions {
		utils.InsertEntry(t, index, i, i%concurrencySalt)
		allEntries[i] = entry.New(i, i%concurrencySalt)
	}

	numThreads := 4
	numSelectsPerThread := 200
	// Select all entries several times concurrently
	doneCh := make(chan bool)
	errCh := make(chan error)
	for range numThreads {
		go selectKeys(index, numSelectsPerThread, allEntries, doneCh, errCh)
	}
	for range numThreads {
		select {
		case <-doneCh:
			continue
		case err := <-errCh:
			t.Fatal(err)
		}
	}
}

// TODO: refactor to use error channel
func insertAndSelectKeys(t *testing.T, table database.Index, c chan int64, done chan bool) {
	for v := range c {
		time.Sleep(jitter())
		err := table.Insert(v, v%concurrencySalt)
		entries, err_select := table.Select()
		if err != nil {
			t.Error("Concurrent insert failed")
		} else if err_select != nil || len(entries) == 0 {
			t.Error("Concurrent select failed")
		}
	}
	done <- true
}

// TODO: refactor to use error channel
func testConcurrentBTreeInsertAndSelect(t *testing.T) {
	index := setupIndex(t, database.BTreeIndexType)

	// Queue entries for insertion
	nums := make(chan int64, 100)
	inserted := make([]int64, 0)
	go func() {
		for i := int64(0); i <= 5000; i++ {
			nums <- i
			inserted = append(inserted, i)
		}
		close(nums)
	}()
	done := make(chan bool)
	numThreads := 4
	for i := 0; i < numThreads; i++ {
		go insertAndSelectKeys(t, index, nums, done)
	}
	for i := 0; i < numThreads; i++ {
		<-done
	}
	// Retrieve entries
	for _, i := range inserted {
		entry, err := index.Find(i)
		if err != nil {
			t.Error(err)
		}
		if entry.Key != i {
			t.Error("Entry with wrong entry was found")
		}
		if entry.Value != i%concurrencySalt {
			t.Error("Entry found has the wrong value")
		}
	}
}
