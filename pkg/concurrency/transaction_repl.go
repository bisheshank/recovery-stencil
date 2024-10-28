package concurrency

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"dinodb/pkg/database"
	"dinodb/pkg/repl"

	"github.com/google/uuid"
)

// Transaction REPL.
func TransactionREPL(db *database.Database, tm *TransactionManager) *repl.REPL {
	r := repl.NewRepl()
	r.AddCommand("create", func(payload string, replConfig *repl.REPLConfig) (string, error) {
		return HandleCreateTable(db, tm, payload, replConfig.GetAddr())
	}, "Create a table. usage: create table <table>")

	r.AddCommand("find", func(payload string, replConfig *repl.REPLConfig) (string, error) {
		return HandleFind(db, tm, payload, replConfig.GetAddr())
	}, "Find an element. usage: find <key> from <table>")

	r.AddCommand("insert", func(payload string, replConfig *repl.REPLConfig) (string, error) {
		return "", HandleInsert(db, tm, payload, replConfig.GetAddr())
	}, "Insert an element. usage: insert <key> <value> into <table>")

	r.AddCommand("update", func(payload string, replConfig *repl.REPLConfig) (string, error) {
		return "", HandleUpdate(db, tm, payload, replConfig.GetAddr())
	}, "Update en element. usage: update <table> <key> <value>")

	r.AddCommand("delete", func(payload string, replConfig *repl.REPLConfig) (string, error) {
		return "", HandleDelete(db, tm, payload, replConfig.GetAddr())
	}, "Delete an element. usage: delete <key> from <table>")

	r.AddCommand("select", func(payload string, replConfig *repl.REPLConfig) (string, error) {
		return HandleSelect(db, tm, payload, replConfig.GetAddr())
	}, "Select elements from a table. usage: select from <table>")

	r.AddCommand("transaction", func(payload string, replConfig *repl.REPLConfig) (string, error) {
		return "", HandleTransaction(db, tm, payload, replConfig.GetAddr())
	}, "Handle transactions. usage: transaction <begin|commit>")

	r.AddCommand("lock", func(payload string, replConfig *repl.REPLConfig) (string, error) {
		return "", HandleLock(db, tm, payload, replConfig.GetAddr())
	}, "Grabs a write lock on a resource. usage: lock <table> <key>")

	r.AddCommand("pretty", func(payload string, replConfig *repl.REPLConfig) (string, error) {
		return HandlePretty(db, payload)
	}, "Print out the internal data representation. usage: pretty")

	return r
}

// Handle transaction.
func HandleTransaction(db *database.Database, tm *TransactionManager, payload string, clientId uuid.UUID) (err error) {
	fields := strings.Fields(payload)
	numFields := len(fields)
	// Usage: create <type> table <table>
	if numFields != 2 || (fields[1] != "begin" && fields[1] != "commit") {
		return errors.New("usage: transaction <begin|commit>")
	}
	switch fields[1] {
	case "begin":
		return tm.Begin(clientId)
	case "commit":
		return tm.Commit(clientId)
	default:
		return errors.New("internal error in create table handler")
	}
}

// Handle create table.
func HandleCreateTable(db *database.Database, tm *TransactionManager, payload string, clientId uuid.UUID) (output string, err error) {
	return database.HandleCreateTable(db, payload)
}

// Handle find.
func HandleFind(db *database.Database, tm *TransactionManager, payload string, clientId uuid.UUID) (output string, err error) {
	fields := strings.Fields(payload)
	numFields := len(fields)
	// Usage: find <key> from <table>
	var key int
	var table database.Index
	if numFields != 4 || fields[2] != "from" {
		return "", fmt.Errorf("usage: find <key> from <table>")
	}
	if key, err = strconv.Atoi(fields[1]); err != nil {
		return "", fmt.Errorf("find error: %v", err)
	}
	if table, err = db.GetTable(fields[3]); err != nil {
		return "", fmt.Errorf("find error: %v", err)
	}
	// Get the transaction, run the find, release lock and rollback if error.
	if err = tm.Lock(clientId, table, int64(key), R_LOCK); err != nil {
		return "", fmt.Errorf("find error: %v", err)
	}
	output, err = database.HandleFind(db, payload)
	if err != nil {
		return "", fmt.Errorf("find error: %v", err)
	}
	return
}

// Handle inserts.
func HandleInsert(db *database.Database, tm *TransactionManager, payload string, clientId uuid.UUID) (err error) {
	fields := strings.Fields(payload)
	numFields := len(fields)
	// Usage: insert <key> <value> into <table>
	var key int
	var table database.Index
	if numFields != 5 || fields[3] != "into" {
		return fmt.Errorf("usage: insert <key> <value> into <table>")
	}
	if key, err = strconv.Atoi(fields[1]); err != nil {
		return fmt.Errorf("insert error: %v", err)
	}
	if table, err = db.GetTable(fields[4]); err != nil {
		return fmt.Errorf("insert error: %v", err)
	}
	// Get the transaction, run the find, release lock and rollback if error.
	if err = tm.Lock(clientId, table, int64(key), W_LOCK); err != nil {
		return fmt.Errorf("insert error: %v", err)
	}
	if err = database.HandleInsert(db, payload); err != nil {
		return fmt.Errorf("insert error: %v", err)
	}
	return nil
}

// Handle update.
func HandleUpdate(db *database.Database, tm *TransactionManager, payload string, clientId uuid.UUID) (err error) {
	fields := strings.Fields(payload)
	numFields := len(fields)
	// Usage: update <table> <key> <value>
	var key int
	var table database.Index
	if numFields != 4 {
		return fmt.Errorf("usage: update <table> <key> <value>")
	}
	if key, err = strconv.Atoi(fields[2]); err != nil {
		return fmt.Errorf("update error: %v", err)
	}
	if table, err = db.GetTable(fields[1]); err != nil {
		return fmt.Errorf("update error: %v", err)
	}
	// Get the transaction, run the find, release lock and rollback if error.
	if err = tm.Lock(clientId, table, int64(key), W_LOCK); err != nil {
		return fmt.Errorf("update error: %v", err)
	}
	if err = database.HandleUpdate(db, payload); err != nil {
		return fmt.Errorf("update error: %v", err)
	}
	return nil
}

// Handle delete.
func HandleDelete(db *database.Database, tm *TransactionManager, payload string, clientId uuid.UUID) (err error) {
	fields := strings.Fields(payload)
	numFields := len(fields)
	// Usage: delete <key> from <table>
	var key int
	var table database.Index
	if numFields != 4 || fields[2] != "from" {
		return fmt.Errorf("usage: delete <key> from <table>")
	}
	if key, err = strconv.Atoi(fields[1]); err != nil {
		return fmt.Errorf("delete error: %v", err)
	}
	if table, err = db.GetTable(fields[3]); err != nil {
		return fmt.Errorf("delete error: %v", err)
	}
	// Get the transaction, run the find, release lock and rollback if error.
	if err = tm.Lock(clientId, table, int64(key), W_LOCK); err != nil {
		return fmt.Errorf("delete error: %v", err)
	}
	if err = database.HandleDelete(db, payload); err != nil {
		return fmt.Errorf("delete error: %v", err)
	}
	return nil
}

// Handle select.
func HandleSelect(db *database.Database, tm *TransactionManager, payload string, clientId uuid.UUID) (output string, err error) {
	fields := strings.Fields(payload)
	numFields := len(fields)
	// Usage: select from <table>
	if numFields != 3 || fields[1] != "from" {
		return "", fmt.Errorf("usage: select from <table>")
	}
	// NOTE: Select is unsafe; not locking anything. May provide an inconsistent view of the database.
	if output, err = database.HandleSelect(db, payload); err != nil {
		return "", fmt.Errorf("select error: %v", err)
	}
	return
}

// Handle write lock requests.
func HandleLock(db *database.Database, tm *TransactionManager, payload string, clientId uuid.UUID) (err error) {
	fields := strings.Fields(payload)
	numFields := len(fields)
	// Usage: lock <table> <key>
	var key int
	var table database.Index
	if numFields != 3 {
		return fmt.Errorf("usage: lock <table> <key>")
	}
	if table, err = db.GetTable(fields[1]); err != nil {
		return fmt.Errorf("lock error: %v", err)
	}
	if key, err = strconv.Atoi(fields[2]); err != nil {
		return fmt.Errorf("lock error: %v", err)
	}
	if err = tm.Lock(clientId, table, int64(key), W_LOCK); err != nil {
		return fmt.Errorf("lock error: %v", err)
	}
	return nil
}

// Handle pretty printing.
func HandlePretty(db *database.Database, payload string) (output string, err error) {
	return database.HandlePretty(db, payload)
}
