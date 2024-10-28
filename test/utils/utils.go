package utils

import (
	"dinodb/pkg/database"
	"dinodb/pkg/entry"
	"math/rand"
	"os"
	"testing"
)

// Mod vals by this value to prevent hardcoding tests
// + 1 is necessary because rand.Int63n(_) can return 0
var Salt int64 = rand.Int63n(1000) + 1

// GetTempDbFile creates a random file in the test's directory to be used for testing,
// returning the file's name. Once the test is done running, the file is deleted
func GetTempDbFile(t *testing.T) string {
	// file will be created in OS's default temporary directory
	tmpfile, err := os.CreateTemp("", "*.db")
	if err != nil {
		t.Fatal(err)
	}

	// Since os.CreateTemp automatically opens the file, we need to close it
	_ = tmpfile.Close()

	EnsureCleanup(t, func() {
		_ = os.Remove(tmpfile.Name())

		// Remove meta database file used for hash indices if it exists
		_ = os.Remove(tmpfile.Name() + ".meta")
	})
	return tmpfile.Name()
}

// InsertEntry tries to insert the entry (key, val) into the specified index,
// erroring the test if the operation fails
func InsertEntry(t *testing.T, index database.Index, key, val int64) {
	err := index.Insert(key, val)
	if err != nil {
		t.Errorf("Failed to insert (%d, %d) into the index: %s", key, val, err)
	}
}

// CheckFindEntry verifies that entry (key, expectedVal) was present in the specified index,
// erroring the test if the entry isn't found or is found with the wrong values
func CheckFindEntry(t *testing.T, index database.Index, key, expectedVal int64) {
	entry, err := index.Find(key)
	if err != nil {
		t.Errorf("Failed to find inserted entry (%d, %d): %s", key, expectedVal, err)
		return
	}

	CheckEntry(t, entry, key, expectedVal)
}

// CheckEntry verifies that the specified entry has the expected key and value,
// erroring the test if this isn't the case
func CheckEntry(t *testing.T, entry entry.Entry, expectedKey, expectedVal int64) {
	if entry.Key != expectedKey {
		t.Errorf("Expected entry to have key %d, but instead found key %d", expectedKey, entry.Key)
		return
	}

	if entry.Value != expectedVal {
		t.Errorf("Expected entry with key %d to have value %d, but instead found value %d", expectedKey, expectedVal, entry.Value)
	}
}
