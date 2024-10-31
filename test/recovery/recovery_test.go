package recovery_test

import (
	"dinodb/test/utils"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"

	"dinodb/pkg/concurrency"
	"dinodb/pkg/config"
	"dinodb/pkg/database"
	"dinodb/pkg/recovery"
)

// =====================================================================
// HELPERS
// =====================================================================
//region helpers

// setupRecovery creates and returns a Database, TransactionManager, RecoveryManager, and a user id to use.
// Uses the specified dbName for the Database's base directory if dbName is not
// the empty string, otherwise uses a unique random base directory
func setupRecovery(t *testing.T, dbName string) (
	*database.Database, *concurrency.TransactionManager, *recovery.RecoveryManager, uuid.UUID) {
	// Create random directory to use for db if none was provided
	var err error
	if dbName == "" {
		t.Parallel()
		dbName, err = os.MkdirTemp("", "")
	}
	if err != nil {
		t.Fatal("Failed to create random database folder:", err)
	}

	// Ensures dbName doesn't have trailing path separator
	dbName = filepath.Clean(dbName)

	d, err := recovery.Prime(dbName)
	if err != nil {
		t.Fatal("Error priming database:", err)
	}

	logFileName := filepath.Join(dbName, config.LogFileName)
	err = d.CreateLogFile(logFileName)
	if err != nil {
		t.Fatal("Error creating log file:", err)
	}

	lm := concurrency.NewResourceLockManager()
	tm := concurrency.NewTransactionManager(lm)
	rm, err := recovery.NewRecoveryManager(d, tm, logFileName)
	if err != nil {
		t.Fatal("Error constructing recovery manager:", err)
	}

	utils.EnsureCleanup(t, func() {
		err = d.Close()
		if err != nil {
			t.Log("Error closing database:", err)
		}
		err = os.RemoveAll(dbName)
		if err != nil {
			t.Log("Error cleaning up database folder:", err)
		}
		recoveryFolderName := dbName + "-recovery"
		_ = os.RemoveAll(recoveryFolderName)
	})
	return d, tm, rm, uuid.New()
}

// crashAndRecover panics, recovers from the panic, re-initializes the Database
// and it's supporting data structures, then recovers using the RecoveryManager
func crashAndRecover(t *testing.T, dbFolderName string) (
	*database.Database, *concurrency.TransactionManager, *recovery.RecoveryManager) {
	func() {
		defer revive(t)
		panic("simulating database crash")
	}()
	d, tm, rm, _ := setupRecovery(t, dbFolderName)
	err := rm.Recover()
	if err != nil {
		t.Fatal("Error recovering using RecoveryManager:", err)
	}
	return d, tm, rm
}

func revive(t *testing.T) {
	if r := recover(); r != nil {
		t.Log("continued from crash:", r)
	}
}

// Creates a table with a random name in the database.
// Returns the name of the table created
func createTable(t *testing.T, db *database.Database, rm *recovery.RecoveryManager, tableType database.IndexType) string {
	tableName := strings.ReplaceAll(uuid.NewString(), "-", "")
	_, err := db.CreateTable(tableName, tableType)
	if err != nil {
		t.Fatal("Error creating table:", err)
	}
	err = rm.Table(string(tableType), tableName)
	if err != nil {
		t.Fatal("Error creating table:", err)
	}
	return tableName
}

func startTransaction(t *testing.T, db *database.Database, tm *concurrency.TransactionManager, rm *recovery.RecoveryManager, clientId uuid.UUID) {
	err := recovery.HandleTransaction(db, tm, rm, "transaction begin", clientId)
	if err != nil {
		t.Fatal("Error starting a transaction:", err)
	}
}

func commitTransaction(t *testing.T, db *database.Database, tm *concurrency.TransactionManager, rm *recovery.RecoveryManager, clientId uuid.UUID) {
	err := recovery.HandleTransaction(db, tm, rm, "transaction commit", clientId)
	if err != nil {
		t.Fatal("Error committing a transaction:", err)
	}
}

func abortTransaction(t *testing.T, tm *concurrency.TransactionManager, rm *recovery.RecoveryManager, clientId uuid.UUID) {
	_, found := tm.GetTransaction(clientId)
	if !found {
		t.Fatal("No transaction found to abort")
	}

	err := rm.Rollback(clientId)
	if err != nil {
		t.Fatal("Error rolling back the transaction:", err)
	}
}

func checkpoint(t *testing.T, rm *recovery.RecoveryManager) {
	err := rm.Checkpoint()
	if err != nil {
		t.Fatal("Error creating a chekpoint:", err)
	}
}

func insertIntoTable(t *testing.T, db *database.Database, tm *concurrency.TransactionManager, rm *recovery.RecoveryManager, clientId uuid.UUID, tableName string, key int64, val int64) {
	payload := fmt.Sprintf("insert %d %d into %s", key, val, tableName)
	err := recovery.HandleInsert(db, tm, rm, payload, clientId)
	if err != nil {
		t.Fatalf("Error inserting (%d, %d) into table %q: %s", key, val, tableName, err)
	}
}

func updateTableEntry(t *testing.T, db *database.Database, tm *concurrency.TransactionManager, rm *recovery.RecoveryManager, clientId uuid.UUID, tableName string, key int64, newVal int64) {
	payload := fmt.Sprintf("update %s %d %d", tableName, key, newVal)
	err := recovery.HandleUpdate(db, tm, rm, payload, clientId)
	if err != nil {
		t.Fatalf("Error updating key %d with new value %d in table %q: %s", key, newVal, tableName, err)
	}
}

func deleteFromTable(t *testing.T, db *database.Database, tm *concurrency.TransactionManager, rm *recovery.RecoveryManager, clientId uuid.UUID, tableName string, key int64) {
	payload := fmt.Sprintf("delete %d from %s", key, tableName)
	err := recovery.HandleDelete(db, tm, rm, payload, clientId)
	if err != nil {
		t.Fatalf("Error deleting %d from %q", key, tableName)
	}
}

// Asserts that finding the specified key fails
func checkFindFails(t *testing.T, db *database.Database, tm *concurrency.TransactionManager, clientId uuid.UUID, tableName string, key int64) {
	table, err := db.GetTable(tableName)
	if err != nil {
		t.Fatalf("Failed to get table %q: %s", tableName, err)
	}
	if err = tm.Lock(clientId, table, key, concurrency.R_LOCK); err != nil {
		t.Fatal("Failed to acquire lock", err)
	}

	_, err = table.Find(key)
	if err == nil {
		t.Errorf("Expected key %d to not be present", key)
	}
}

func checkFind(t *testing.T, db *database.Database, tm *concurrency.TransactionManager, clientId uuid.UUID, tableName string, key, expectedVal int64) {
	table, err := db.GetTable(tableName)
	if err != nil {
		t.Fatalf("Failed to get table %q: %s", tableName, err)
	}
	if err = tm.Lock(clientId, table, key, concurrency.R_LOCK); err != nil {
		t.Fatal("Failed to acquire lock", err)
	}

	entry, err := table.Find(key)
	if err != nil {
		t.Errorf("Expected key %d to not be present", key)
	}

	if entry.Value != expectedVal {
		t.Errorf("Expected to find value %d under key %d in table %q, but instead found %d", expectedVal, key, tableName, entry.Value)
	}
}

//endregion

// =====================================================================
// TESTS
// =====================================================================

func TestRecovery(t *testing.T) {
	t.Run("Basic", testBasic)
	t.Run("Abort", testAbort)
	t.Run("InsertAbort", testInsertAbort)
	t.Run("AbortInsertDeleteAndUpdate", testAbortInsertDeleteAndUpdate)
	t.Run("AbortIsolated", testAbortIsolated)
	t.Run("InsertCommit", testInsertCommit)
	t.Run("InsertDeleteCommit", testInsertDeleteCommit)
	t.Run("InsertCommitUpdate", testInsertCommitUpdate)
	t.Run("InsertCheckpointCommitUpdate", testInsertCheckpointCommitUpdate)
	t.Run("MultipleTablesOneClient", testMultipleTablesOneClient)
	t.Run("MultiInsertCheckpointing", testMultiInsertCheckpointing)
	t.Run("MultiInsertCommitDeleteCheckpointing", testMultiInsertCommitDeleteCheckpointing)
}

func testBasic(t *testing.T) {
	db, tm, rm, clientId := setupRecovery(t, "")
	tableName := createTable(t, db, rm, database.BTreeIndexType)
	startTransaction(t, db, tm, rm, clientId)
	insertIntoTable(t, db, tm, rm, clientId, tableName, 0, 0)

	commitTransaction(t, db, tm, rm, clientId)
}

func testAbort(t *testing.T) {
	db, tm, rm, clientId := setupRecovery(t, "")
	createTable(t, db, rm, database.BTreeIndexType)
	startTransaction(t, db, tm, rm, clientId)

	abortTransaction(t, tm, rm, clientId)
}

func testInsertAbort(t *testing.T) {
	db, tm, rm, clientId := setupRecovery(t, "")
	tableName := createTable(t, db, rm, database.BTreeIndexType)
	startTransaction(t, db, tm, rm, clientId)
	insertIntoTable(t, db, tm, rm, clientId, tableName, 0, 0)

	abortTransaction(t, tm, rm, clientId)

	startTransaction(t, db, tm, rm, clientId)
	checkFindFails(t, db, tm, clientId, tableName, 0)
}

func testAbortInsertDeleteAndUpdate(t *testing.T) {
	db, tm, rm, clientId := setupRecovery(t, "")
	tableName := createTable(t, db, rm, database.BTreeIndexType)
	startTransaction(t, db, tm, rm, clientId)
	insertIntoTable(t, db, tm, rm, clientId, tableName, 0, 0)
	updateTableEntry(t, db, tm, rm, clientId, tableName, 0, 1)
	deleteFromTable(t, db, tm, rm, clientId, tableName, 0)

	abortTransaction(t, tm, rm, clientId)

	startTransaction(t, db, tm, rm, clientId)
	checkFindFails(t, db, tm, clientId, tableName, 0)
}

func testAbortIsolated(t *testing.T) {
	db, tm, rm, clientId1 := setupRecovery(t, "")
	clientId2 := uuid.New()
	tableName := createTable(t, db, rm, database.BTreeIndexType)
	startTransaction(t, db, tm, rm, clientId1)
	startTransaction(t, db, tm, rm, clientId2)
	insertIntoTable(t, db, tm, rm, clientId1, tableName, 0, 0)
	insertIntoTable(t, db, tm, rm, clientId2, tableName, 1, 1)

	abortTransaction(t, tm, rm, clientId1)

	checkFindFails(t, db, tm, clientId2, tableName, 0)
	checkFind(t, db, tm, clientId2, tableName, 1, 1)
}

func testInsertCommit(t *testing.T) {
	// Define the test cases. Maps subtest name to the number of entries
	tests := map[string]int64{
		"Single": 1,
		"Many":   500,
	}

	for name, numEntries := range tests {
		t.Run(name, func(t *testing.T) {
			db, tm, rm, clientId := setupRecovery(t, "")
			// Before crash
			tableName := createTable(t, db, rm, database.BTreeIndexType)
			startTransaction(t, db, tm, rm, clientId)
			for i := int64(0); i < numEntries; i++ {
				insertIntoTable(t, db, tm, rm, clientId, tableName, i, i%utils.Salt)
			}
			commitTransaction(t, db, tm, rm, clientId)

			db, tm, rm = crashAndRecover(t, db.GetBasePath())
			// After crash
			startTransaction(t, db, tm, rm, clientId)
			for i := int64(0); i < numEntries; i++ {
				checkFind(t, db, tm, clientId, tableName, i, i%utils.Salt)
			}
		})
	}
}

func testInsertDeleteCommit(t *testing.T) {
	db, tm, rm, clientId := setupRecovery(t, "")
	// Before crash
	tableName := createTable(t, db, rm, database.BTreeIndexType)
	startTransaction(t, db, tm, rm, clientId)
	insertIntoTable(t, db, tm, rm, clientId, tableName, 0, 0)
	deleteFromTable(t, db, tm, rm, clientId, tableName, 0)
	commitTransaction(t, db, tm, rm, clientId)

	db, tm, rm = crashAndRecover(t, db.GetBasePath())
	// After crash
	startTransaction(t, db, tm, rm, clientId)
	checkFindFails(t, db, tm, clientId, tableName, 0)
}

func testInsertCommitUpdate(t *testing.T) {
	db, tm, rm, clientId := setupRecovery(t, "")
	// Before crash
	tableName := createTable(t, db, rm, database.BTreeIndexType)
	startTransaction(t, db, tm, rm, clientId)
	insertIntoTable(t, db, tm, rm, clientId, tableName, 0, 0)
	commitTransaction(t, db, tm, rm, clientId)
	startTransaction(t, db, tm, rm, clientId)
	updateTableEntry(t, db, tm, rm, clientId, tableName, 0, 1)

	db, tm, rm = crashAndRecover(t, db.GetBasePath())
	// After crash
	startTransaction(t, db, tm, rm, clientId)
	checkFind(t, db, tm, clientId, tableName, 0, 0)
}

func testInsertCheckpointCommitUpdate(t *testing.T) {
	db, tm, rm, clientId := setupRecovery(t, "")
	// Before crash
	tableName := createTable(t, db, rm, database.BTreeIndexType)
	startTransaction(t, db, tm, rm, clientId)
	insertIntoTable(t, db, tm, rm, clientId, tableName, 0, 0)
	checkpoint(t, rm)
	commitTransaction(t, db, tm, rm, clientId)
	startTransaction(t, db, tm, rm, clientId)
	updateTableEntry(t, db, tm, rm, clientId, tableName, 0, 1)

	db, tm, rm = crashAndRecover(t, db.GetBasePath())
	// After crash
	startTransaction(t, db, tm, rm, clientId)
	checkFind(t, db, tm, clientId, tableName, 0, 0)
}

func testMultipleTablesOneClient(t *testing.T) {
	db, tm, rm, clientId := setupRecovery(t, "")
	// Before crash
	tableName1 := createTable(t, db, rm, database.BTreeIndexType)
	tableName2 := createTable(t, db, rm, database.BTreeIndexType)
	startTransaction(t, db, tm, rm, clientId)
	insertIntoTable(t, db, tm, rm, clientId, tableName1, 1, 1)
	insertIntoTable(t, db, tm, rm, clientId, tableName2, 2, 2)
	commitTransaction(t, db, tm, rm, clientId)

	db, tm, rm = crashAndRecover(t, db.GetBasePath())
	// After crash
	startTransaction(t, db, tm, rm, clientId)
	checkFind(t, db, tm, clientId, tableName1, 1, 1)
	checkFindFails(t, db, tm, clientId, tableName1, 2)
	checkFindFails(t, db, tm, clientId, tableName2, 1)
	checkFind(t, db, tm, clientId, tableName2, 2, 2)
}

func testMultiInsertCheckpointing(t *testing.T) {
	db, tm, rm, clientId := setupRecovery(t, "")
	numEntries := int64(500)
	// Before crash
	tableName := createTable(t, db, rm, database.BTreeIndexType)
	startTransaction(t, db, tm, rm, clientId)
	// insert all, checkpointing every 100 entries - but don't commit
	for i := int64(0); i < numEntries; i++ {
		insertIntoTable(t, db, tm, rm, clientId, tableName, i, i%utils.Salt)
		if i%100 == 0 {
			checkpoint(t, rm)
		}
	}

	db, tm, rm = crashAndRecover(t, db.GetBasePath())
	// After crash, shouldn't be able to find any entries since none committed
	startTransaction(t, db, tm, rm, clientId)
	for i := int64(0); i < numEntries; i++ {
		checkFindFails(t, db, tm, clientId, tableName, i)
	}
}

func testMultiInsertCommitDeleteCheckpointing(t *testing.T) {
	db, tm, rm, clientId := setupRecovery(t, "")
	numEntries := int64(500)
	// Before crash
	tableName := createTable(t, db, rm, database.BTreeIndexType)
	startTransaction(t, db, tm, rm, clientId)
	// insert all, checkpointing every 100 entries
	for i := int64(0); i < numEntries; i++ {
		insertIntoTable(t, db, tm, rm, clientId, tableName, i, i%utils.Salt)
		if i%100 == 0 {
			checkpoint(t, rm)
		}
	}
	commitTransaction(t, db, tm, rm, clientId)
	startTransaction(t, db, tm, rm, clientId)
	// start deleting all, checkpointing as we go - but don't commit
	for i := int64(0); i < numEntries; i++ {
		deleteFromTable(t, db, tm, rm, clientId, tableName, i)
		if i%100 == 0 {
			checkpoint(t, rm)
		}
	}

	db, tm, rm = crashAndRecover(t, db.GetBasePath())
	// After crash, should be able to find all entries since the deletes didn't commit
	startTransaction(t, db, tm, rm, clientId)
	for i := int64(0); i < numEntries; i++ {
		checkFind(t, db, tm, clientId, tableName, i, i%utils.Salt)
	}
}
