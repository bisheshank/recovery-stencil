package hash_test

import (
	"math/rand"
	"testing"

	"dinodb/pkg/hash"
	"dinodb/test/utils"
)

// =====================================================================
// HELPERS
// =====================================================================

// Mod vals by this value to prevent hardcoding tests
var hashSalt = utils.Salt

// setupHash creates and opens an empty HashIndex
func setupHash(t *testing.T) *hash.HashIndex {
	t.Parallel()
	dbName := utils.GetTempDbFile(t)
	index, err := hash.OpenTable(dbName)
	if err != nil {
		t.Fatal("Failed to create hash index:", err)
	}

	return index
}

// closeAndReopen closes and reopens the specified HashIndex,
// which should trigger writing/reading it's data from disk
func closeAndReopen(t *testing.T, index *hash.HashIndex) *hash.HashIndex {
	err := index.Close()
	if err != nil {
		t.Fatal("Failed to close hash index:", err)
	}

	reopenedIndex, err := hash.OpenTable(index.GetPager().GetFileName())
	if err != nil {
		t.Error("Failed to reopen hash index:", err)
	}

	return reopenedIndex
}

// Maps subtest name to the InsertTestData to use
type InsertTestsMap map[string]InsertTestData

type InsertTestData struct {
	numInserts  int64 // how many insertions to execute
	writeToDisk bool  // whether to write to disk
}

// =====================================================================
// TESTS
// =====================================================================

func TestHashInsert(t *testing.T) {
	t.Run("Splitting", testHashSplitting)
	t.Run("Ascending", testInsertAscending)
	t.Run("Random", testInsertRandom)
}

/*
Creates a Hash index, sets up 16 channels and go routines to compute hashes,
and inserts entries into the hash index until a global depth of 4 is reached.
Continues to insert values and then finds specific entries and validates that
they are correct.
*/
func testHashSplitting(t *testing.T) {
	index := setupHash(t)

	toFind := make(map[int64]int64)
	// Set up adverserial workload
	targetDepth := int64(4)
	var nums [16]chan int64
	for i := range nums {
		nums[i] = make(chan int64)
		go func(target int64) {
			for testNum := int64(0); ; testNum++ {
				if hash.Hasher(testNum, targetDepth) == target {
					nums[target] <- testNum
				}
			}
		}(int64(i))
	}
	for index.GetTable().GetDepth() < targetDepth {
		nextNum := <-nums[0]
		toFind[nextNum] = nextNum % hashSalt
		utils.InsertEntry(t, index, nextNum, nextNum%hashSalt)
	}
	targetVal := <-nums[15]
	toFind[targetVal] = targetVal % hashSalt
	utils.InsertEntry(t, index, targetVal, targetVal%hashSalt)

	canaryVal := <-nums[7]

	for {
		nextNum := <-nums[3]
		toFind[nextNum] = nextNum % hashSalt
		utils.InsertEntry(t, index, nextNum, nextNum%hashSalt)

		nextNum = <-nums[7]
		toFind[nextNum] = nextNum % hashSalt
		utils.InsertEntry(t, index, nextNum, nextNum%hashSalt)

		// Check if we've inserted enough values
		hash := hash.Hasher(canaryVal, index.GetTable().GetDepth())
		bucket, err := index.GetTable().GetBucket(hash)
		if err != nil {
			continue
		}
		ok := bucket.GetDepth() >= 3
		index.GetPager().PutPage(bucket.GetPage())
		if ok {
			break
		}
	}

	utils.CheckFindEntry(t, index, targetVal, targetVal%hashSalt)
	for k, v := range toFind {
		utils.CheckFindEntry(t, index, k, v)
	}
	index.Close()
}

// Given InsertTestData, stages a testing function to insert ascending entries.
func stageInsertAscending(testData InsertTestData) func(t *testing.T) {
	return func(t *testing.T) {
		index := setupHash(t)
		secondSalt := rand.Int63n(1000)

		// Insert entries
		for i := range testData.numInserts {
			utils.InsertEntry(t, index, i, (i*secondSalt)%hashSalt)
		}

		// Stop the test if any insertions failed
		if t.Failed() {
			t.FailNow()
		}

		// If the test case calls for it, close and reopen the index to trigger writing/reading data from disk
		if testData.writeToDisk {
			index = closeAndReopen(t, index)
		}

		// Retrieve and check entries
		for i := range testData.numInserts {
			utils.CheckFindEntry(t, index, i, (i*secondSalt)%hashSalt)
		}
		index.Close()
	}
}

// Inserts a variable number of ascending keys and somewhat ascending values into a HashIndex,
// checking that they can be found with and without closing/flushing the index's data to disk
func testInsertAscending(t *testing.T) {
	// Define the test cases.
	insertAscendingTests := InsertTestsMap{
		"TenNoWrite":        {10, false},
		"TenWithWrite":      {10, true},
		"ThousandNoWrite":   {1000, false},
		"ThousandWithWrite": {1000, true},
	}

	// Run the tests.
	for name, testData := range insertAscendingTests {
		t.Run(name, stageInsertAscending(testData))
	}
}

// Given InsertTestData, stages a testing function for inserting random entries
func stageInsertRandom(testData InsertTestData) func(t *testing.T) {
	return func(t *testing.T) {
		index := setupHash(t)
		// Generate and insert entries
		entries, answerKey := utils.GenerateRandomKeyValuePairs(testData.numInserts)
		for _, entry := range entries {
			utils.InsertEntry(t, index, entry.Key, entry.Val)
		}

		// Stop the test if any insertions failed
		if t.Failed() {
			t.FailNow()
		}

		// If the test case calls for it, close and reopen the index to trigger writing/reading data from disk
		if testData.writeToDisk {
			index = closeAndReopen(t, index)
		}

		// Retrieve and check entries
		for k, v := range answerKey {
			utils.CheckFindEntry(t, index, k, v)
		}
		index.Close()
	}
}

// Inserts a variable number of random keys and values into a BTreeIndex,
// checking that they can be found with and without closing/flushing the index's data to disk
func testInsertRandom(t *testing.T) {
	// Define the test cases.
	tests := InsertTestsMap{
		"ThousandNoWrite":   {1000, false},
		"ThousandWithWrite": {1000, true},
	}

	// Run the tests.
	for name, testData := range tests {
		t.Run(name, stageInsertRandom(testData))
	}
}

// TODO: add test that duplicate keys are allowed
