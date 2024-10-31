package recovery

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"dinodb/pkg/concurrency"
	"dinodb/pkg/database"
	"dinodb/pkg/repl"

	"github.com/google/uuid"
)

// Recovery REPL.
func RecoveryREPL(db *database.Database, tm *concurrency.TransactionManager, rm *RecoveryManager) *repl.REPL {
	r := repl.NewRepl()
	r.AddCommand("create", func(payload string, replConfig *repl.REPLConfig) (string, error) {
		return HandleCreateTable(db, rm, payload)
	}, "Create a table. usage: create <btree|hash> table <table>")

	r.AddCommand("find", func(payload string, replConfig *repl.REPLConfig) (string, error) {
		return HandleFind(db, tm, rm, payload, replConfig.GetAddr())
	}, "Find an element. usage: find <key> from <table>")

	r.AddCommand("insert", func(payload string, replConfig *repl.REPLConfig) (string, error) {
		return "", HandleInsert(db, tm, rm, payload, replConfig.GetAddr())
	}, "Insert an element. usage: insert <key> <value> into <table>")

	r.AddCommand("update", func(payload string, replConfig *repl.REPLConfig) (string, error) {
		return "", HandleUpdate(db, tm, rm, payload, replConfig.GetAddr())
	}, "Update en element. usage: update <table> <key> <value>")

	r.AddCommand("delete", func(payload string, replConfig *repl.REPLConfig) (string, error) {
		return "", HandleDelete(db, tm, rm, payload, replConfig.GetAddr())
	}, "Delete an element. usage: delete <key> from <table>")

	r.AddCommand("select", func(payload string, replConfig *repl.REPLConfig) (string, error) {
		return HandleSelect(db, tm, rm, payload, replConfig.GetAddr())
	}, "Select elements from a table. usage: select from <table>")

	r.AddCommand("transaction", func(payload string, replConfig *repl.REPLConfig) (string, error) {
		return "", HandleTransaction(db, tm, rm, payload, replConfig.GetAddr())
	}, "Handle transactions. usage: transaction <begin|commit>")

	r.AddCommand("lock", func(payload string, replConfig *repl.REPLConfig) (string, error) {
		return "", HandleLock(db, tm, payload, replConfig.GetAddr())
	}, "Grabs a write lock on a resource. usage: lock <table> <key>")

	r.AddCommand("checkpoint", func(payload string, replConfig *repl.REPLConfig) (string, error) {
		return "", HandleCheckpoint(db, tm, rm, payload, replConfig.GetAddr())
	}, "Saves a checkpoint of the current database state and running transactions. usage: checkpoint")

	r.AddCommand("abort", func(payload string, replConfig *repl.REPLConfig) (string, error) {
		return "", HandleAbort(db, tm, rm, payload, replConfig.GetAddr())
	}, "Simulate an abort of the current transaction. usage: abort")

	r.AddCommand("crash", func(payload string, replConfig *repl.REPLConfig) (string, error) {
		return "", HandleCrash(db, tm, rm, payload, replConfig.GetAddr())
	}, "Crash the database. usage: crash")

	r.AddCommand("pretty", func(payload string, replConfig *repl.REPLConfig) (string, error) {
		return HandlePretty(db, payload)
	}, "Print out the internal data representation. usage: pretty")

	return r
}

// Handle transaction.
func HandleTransaction(db *database.Database, tm *concurrency.TransactionManager, rm *RecoveryManager, payload string, clientId uuid.UUID) (err error) {
	fields := strings.Fields(payload)
	numFields := len(fields)
	// Usage: transaction <begin|commit>
	if numFields != 2 || (fields[1] != "begin" && fields[1] != "commit") {
		return errors.New("usage: transaction <begin|commit>")
	}
	switch fields[1] {
	case "begin":
		err = rm.Start(clientId)
		if err != nil {
			return err
		}
		err = tm.Begin(clientId)
	case "commit":
		err = rm.Commit(clientId)
		if err != nil {
			return err
		}
		err = tm.Commit(clientId)
	default:
		return errors.New("internal error in create table handler")
	}
	if err != nil {
		rberr := rm.Rollback(clientId)
		if rberr != nil {
			return rberr
		}
	}
	return err
}

// Handle create table.
func HandleCreateTable(db *database.Database, rm *RecoveryManager, payload string) (output string, err error) {
	fields := strings.Fields(payload)
	numFields := len(fields)
	// Usage: create <type> table <table>
	if numFields != 4 || fields[2] != "table" || (fields[1] != "btree" && fields[1] != "hash") {
		return "", fmt.Errorf("usage: create <btree|hash> table <table>")
	}
	err = rm.Table(fields[1], fields[3])
	if err != nil {
		return "", err
	}
	return database.HandleCreateTable(db, payload)
}

// Handle find.
func HandleFind(db *database.Database, tm *concurrency.TransactionManager, rm *RecoveryManager, payload string, clientId uuid.UUID) (output string, err error) {
	return concurrency.HandleFind(db, tm, payload, clientId)
}

// Handle insert.
func HandleInsert(db *database.Database, tm *concurrency.TransactionManager, rm *RecoveryManager, payload string, clientId uuid.UUID) (err error) {
	fields := strings.Fields(payload)
	numFields := len(fields)
	// Usage: insert <key> <value> into <table>
	var key, newval int
	var table database.Index
	if numFields != 5 || fields[3] != "into" {
		return fmt.Errorf("usage: insert <key> <value> into <table>")
	}
	if key, err = strconv.Atoi(fields[1]); err != nil {
		return fmt.Errorf("insert error: %v", err)
	}
	if newval, err = strconv.Atoi(fields[2]); err != nil {
		return fmt.Errorf("insert error: %v", err)
	}
	if table, err = db.GetTable(fields[4]); err != nil {
		return fmt.Errorf("insert error: %v", err)
	}
	// First, check that the desired value doesn't exist.
	_, err = table.Find(int64(key))
	if err == nil {
		return errors.New("insert error: key already exists")
	}
	// Log.
	err = rm.Edit(clientId, table, INSERT_ACTION, int64(key), 0, int64(newval))
	if err != nil {
		return err
	}
	// Run transaction insert.
	err = concurrency.HandleInsert(db, tm, payload, clientId)
	if err != nil {
		// Add a log to mark this insert as a no-op.
		ederr := rm.Edit(clientId, table, DELETE_ACTION, int64(key), int64(newval), int64(0))
		if ederr != nil {
			return fmt.Errorf("error marking insert as no-op: %w", ederr)
		}
		// Then pop the last two actions from the transaction stack because
		// these last two actions were no-ops.
		stack := rm.txStack[clientId]
		rm.txStack[clientId] = stack[:len(stack)-2]
		rberr := rm.Rollback(clientId)
		if rberr != nil {
			return rberr
		}
	}
	return err
}

// Handle update.
func HandleUpdate(db *database.Database, tm *concurrency.TransactionManager, rm *RecoveryManager, payload string, clientId uuid.UUID) (err error) {
	fields := strings.Fields(payload)
	numFields := len(fields)
	// Usage: update <table> <key> <value>
	var key, newval int
	var table database.Index
	if numFields != 4 {
		return fmt.Errorf("usage: update <table> <key> <value>")
	}
	if key, err = strconv.Atoi(fields[2]); err != nil {
		return fmt.Errorf("update error: %v", err)
	}
	if newval, err = strconv.Atoi(fields[3]); err != nil {
		return fmt.Errorf("update error: %v", err)
	}
	if table, err = db.GetTable(fields[1]); err != nil {
		return fmt.Errorf("update error: %v", err)
	}
	// First, check that the desired value exists.
	oldval, err := table.Find(int64(key))
	if err != nil {
		return errors.New("update error: key doesn't exists")
	}
	// Log.
	err = rm.Edit(clientId, table, UPDATE_ACTION, int64(key), oldval.Value, int64(newval))
	if err != nil {
		return err
	}
	// Run transaction insert.
	err = concurrency.HandleUpdate(db, tm, payload, clientId)
	if err != nil {
		// Add a log to mark this update as a no-op.
		ederr := rm.Edit(clientId, table, UPDATE_ACTION, int64(key), int64(newval), oldval.Value)
		if ederr != nil {
			return fmt.Errorf("error marking update as no-op: %w", ederr)
		}
		// Then pop the last two actions from the transaction stack because
		// these last two actions were no-ops.
		stack := rm.txStack[clientId]
		rm.txStack[clientId] = stack[:len(stack)-2]
		rberr := rm.Rollback(clientId)
		if rberr != nil {
			return rberr
		}
	}
	return err
}

// Handle delete.
func HandleDelete(db *database.Database, tm *concurrency.TransactionManager, rm *RecoveryManager, payload string, clientId uuid.UUID) (err error) {
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
	// First, check that the desired value exists.
	oldval, err := table.Find(int64(key))
	if err != nil {
		return errors.New("delete error: key doesn't exists")
	}
	// Log.
	err = rm.Edit(clientId, table, DELETE_ACTION, int64(key), oldval.Value, 0)
	if err != nil {
		return err
	}
	// Run transaction insert.
	err = concurrency.HandleDelete(db, tm, payload, clientId)
	if err != nil {
		// Add a log to mark this delete as a no-op.
		ederr := rm.Edit(clientId, table, INSERT_ACTION, int64(key), 0, oldval.Value)
		if ederr != nil {
			return fmt.Errorf("error marking delete as no-op: %w", ederr)
		}
		// Then pop the last two actions from the transaction stack because
		// these last two actions were no-ops.
		stack := rm.txStack[clientId]
		rm.txStack[clientId] = stack[:len(stack)-2]
		rberr := rm.Rollback(clientId)
		if rberr != nil {
			return rberr
		}
	}
	return err
}

// Handle select.
func HandleSelect(db *database.Database, tm *concurrency.TransactionManager, rm *RecoveryManager, payload string, clientId uuid.UUID) (output string, err error) {
	fields := strings.Fields(payload)
	numFields := len(fields)
	// Usage: select from <table>
	if numFields != 3 || fields[1] != "from" {
		return "", fmt.Errorf("usage: select from <table>")
	}
	// NOTE: Select is unsafe; not locking anything. May provide an inconsistent view of the database.
	output, err = database.HandleSelect(db, payload)
	return
}

// Handle write lock requests.
func HandleLock(db *database.Database, tm *concurrency.TransactionManager, payload string, clientId uuid.UUID) (err error) {
	return concurrency.HandleLock(db, tm, payload, clientId)
}

// Handle checkpoint.
func HandleCheckpoint(db *database.Database, tm *concurrency.TransactionManager, rm *RecoveryManager, payload string, clientId uuid.UUID) (err error) {
	fields := strings.Fields(payload)
	numFields := len(fields)
	// Usage: checkpoint
	if numFields != 1 {
		return fmt.Errorf("usage: checkpoint")
	}
	// Get the transaction, run the find, release lock and rollback if error.
	err = rm.Checkpoint()
	if err != nil {
		return err
	}
	return err
}

// Handle abort.
func HandleAbort(db *database.Database, tm *concurrency.TransactionManager, rm *RecoveryManager, payload string, clientId uuid.UUID) (err error) {
	fields := strings.Fields(payload)
	numFields := len(fields)
	// Usage: abort
	if numFields != 1 {
		return fmt.Errorf("usage: abort")
	}
	// Get the transaction, run the find, release lock and rollback if error.
	_, found := tm.GetTransaction(clientId)
	if !found {
		return errors.New("no running transaction to abort")
	}
	err = rm.Rollback(clientId)
	return err
}

// Handle crash.
func HandleCrash(db *database.Database, tm *concurrency.TransactionManager, rm *RecoveryManager, payload string, clientId uuid.UUID) (err error) {
	fields := strings.Fields(payload)
	numFields := len(fields)
	// Usage: crash
	if numFields != 1 {
		return fmt.Errorf("usage: crash")
	}
	panic("it's the end of the world!")
}

// Handle pretty printing.
func HandlePretty(db *database.Database, payload string) (output string, err error) {
	return database.HandlePretty(db, payload)
}
