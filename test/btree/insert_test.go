package btree_test

import (
	"math/rand"
	"testing"

	"dinodb/pkg/btree"
	"dinodb/test/utils"
)

// =====================================================================
// HELPERS
// =====================================================================

// Mod vals by this value to prevent hardcoding tests
var btreeSalt int64 = utils.Salt

// setupBTree creates and opens an empty BTreeIndex
func setupBTree(t *testing.T) *btree.BTreeIndex {
	t.Parallel()
	dbName := utils.GetTempDbFile(t)
	index, err := btree.OpenIndex(dbName)
	if err != nil {
		t.Fatal("Failed to create BTree index:", err)
	}

	return index
}

var btreeSecondarySalt int64 = rand.Int63n(1000)

// Given a key, deterministically generates a "random" value based on a salt.
// This helper allows us to change how we randomize values without having to update the code extensively
func generateValue(key int64) int64 {
	return (key * btreeSecondarySalt) % btreeSalt
}

// standardBTreeSetup creates a new BTree index and inserts entries with
// keys 0 to numInserts-1 and values determined by generateValue
func standardBTreeSetup(t *testing.T, numInserts int64) *btree.BTreeIndex {
	index := setupBTree(t)

	// Insert entries
	for i := range numInserts {
		utils.InsertEntry(t, index, i, generateValue(i))
	}
	// Stop the test if any insertions failed
	if t.Failed() {
		t.FailNow()
	}

	return index
}

// closeAndReopen closes and reopens the specified BTreeIndex,
// which should trigger writing/reading it's data from disk
func closeAndReopen(t *testing.T, index *btree.BTreeIndex) *btree.BTreeIndex {
	err := index.Close()
	if err != nil {
		t.Fatal("Failed to close hash index:", err)
	}

	reopenedIndex, err := btree.OpenIndex(index.GetPager().GetFileName())
	if err != nil {
		t.Error("Failed to reopen hash index:", err)
	}

	return reopenedIndex
}

type InsertTestData struct {
	numInserts  int64 // how many insertions to execute
	writeToDisk bool  // whether to write to disk
}

// =====================================================================
// TESTS
// =====================================================================

func TestBTreeInsert(t *testing.T) {
	t.Run("Ascending", testInsertAscending)
	t.Run("Random", testInsertRandom)
	t.Run("Duplicates", testInsertDuplicateKeys)
}

func stageInsertAscending(testData InsertTestData) func(t *testing.T) {
	return func(t *testing.T) {
		index := standardBTreeSetup(t, testData.numInserts)

		// If the test case calls for it, close and reopen the index to trigger writing/reading data from disk
		if testData.writeToDisk {
			index = closeAndReopen(t, index)
		}

		// Retrieve and check entries
		for i := range testData.numInserts {
			utils.CheckFindEntry(t, index, i, generateValue(i))
		}
		index.Close()
	}
}

// Inserts a variable number of ascending keys and somewhat ascending values into a BTreeIndex,
// checking that they can be found with and without closing/flushing the index's data to disk
func testInsertAscending(t *testing.T) {
	// Define the test cases
	tests := map[string]InsertTestData{
		"TenNoWrite":        {10, false},
		"TenWithWrite":      {10, true},
		"ThousandNoWrite":   {1000, false},
		"ThousandWithWrite": {1000, true},
	}

	// Runs the test cases
	for name, testData := range tests {
		t.Run(name, stageInsertAscending(testData))
	}
}

func stageInsertRandom(testData InsertTestData) func(t *testing.T) {
	return func(t *testing.T) {
		index := setupBTree(t)

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
	// Define the test cases
	tests := map[string]InsertTestData{
		"ThousandNoWrite":   {1000, false},
		"ThousandWithWrite": {1000, true},
	}

	// Run the test cases
	for name, testData := range tests {
		t.Run(name, stageInsertRandom(testData))
	}
}

/*
Creates a BTree index, inserts a thousand entries, then tries to insert a
thousand duplicate entries and checks that they fail, then closes and reopens
the database and tries to insert a thousand duplicates again and checks that
the duplicate inserts fail.
*/
func testInsertDuplicateKeys(t *testing.T) {
	numInserts := int64(1000)
	index := standardBTreeSetup(t, numInserts)

	// Try inserting duplicates
	for i := range numInserts {
		err := index.Insert(i, i)
		if err == nil {
			t.Fatalf("Could insert duplicate key %d into a B+Tree", i)
		}
	}

	// Close and reopen the index to trigger writing/reading data from disk
	index = closeAndReopen(t, index)

	// Try inserting duplicates again
	for i := range numInserts {
		err := index.Insert(i, i)
		if err == nil {
			t.Fatalf("Could insert duplicate key %d into a B+Tree after writing", i)
		}
	}
	index.Close()
}
