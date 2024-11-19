package recovery

import (
	"bytes"
	"errors"
	"fmt"
	// "go/types"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"dinodb/pkg/concurrency"
	"dinodb/pkg/config"
	"dinodb/pkg/database"

	"github.com/icza/backscanner"
	"github.com/otiai10/copy"

	"github.com/google/uuid"
)

// RecoveryManager is the construct that manages the write-ahead log for a database.
// It is therefore responsible for recovery from crashes and rolling back uncommitted transactions.
type RecoveryManager struct {
	db *database.Database              // The underlying database that this recovery manager is for.
	tm *concurrency.TransactionManager // The transaction manager used for this database.

	// Keeps track of the operations of all uncommitted transactions.
	// Maps each client/transaction id to a stack of logs.
	txStack map[uuid.UUID][]editLog

	logFile *os.File   // The log file where the write-ahead log is stored.
	mtx     sync.Mutex // A mutex used for allowing safe concurrent use of this struct.
}

// NewRecoveryManager returns a new recovery manager for the specified database,
// transaction manager, and using the specified log file.
// Returns an error instead if the log file couldn't be opened.
func NewRecoveryManager(
	db *database.Database,
	tm *concurrency.TransactionManager,
	logFilename string,
) (*RecoveryManager, error) {
	logFile, err := os.OpenFile(logFilename, os.O_APPEND|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	return &RecoveryManager{
		db:      db,
		tm:      tm,
		txStack: make(map[uuid.UUID][]editLog),
		logFile: logFile,
	}, nil
}

// flushLog serializes the specified log and immediately appends it
// to the end of log file on disk. Expects rm.mtx to be locked.
func (rm *RecoveryManager) flushLog(log log) error {
	_, err := rm.logFile.WriteString(log.toString())
	if err != nil {
		return err
	}
	err = rm.logFile.Sync()
	return err
}

// Table records the creation of a table to the write-ahead log.
func (rm *RecoveryManager) Table(tblType string, tblName string) error {
	rm.mtx.Lock()
	defer rm.mtx.Unlock()
	tl := tableLog{
		tblType: tblType,
		tblName: tblName,
	}
	err := rm.flushLog(tl)
	if err != nil {
		return fmt.Errorf("error writing a Table log: %w", err)
	}
	return nil
}

// Edit records an individual entry change (insert, update, deletion) to the write-ahead log.
func (rm *RecoveryManager) Edit(clientId uuid.UUID, table database.Index, action action, key int64, oldval int64, newval int64) error {
	rm.mtx.Lock()
	defer rm.mtx.Unlock()

	el := editLog {
		id: clientId,
		tablename: table.GetName(),
		action: action,
		key: key,
		oldval: oldval,
		newval: newval,
	}

	rm.txStack[clientId] = append(rm.txStack[clientId], el)

	return rm.flushLog(el)
}

// Start records the start of a transaction to the write-ahead log.
func (rm *RecoveryManager) Start(clientId uuid.UUID) error {
	rm.mtx.Lock()
	defer rm.mtx.Unlock()

	rm.txStack[clientId] = make([]editLog, 0)

	sl := startLog{
		id: clientId,
	}

	return rm.flushLog(sl)
}

// Commit records the committing of a transaction to the write-ahead log.
func (rm *RecoveryManager) Commit(clientId uuid.UUID) error {
	rm.mtx.Lock()
	defer rm.mtx.Unlock()

	delete(rm.txStack, clientId)

	cl := commitLog{
		id: clientId,
	}

	return rm.flushLog(cl)
}

// Checkpoint flushes all pages to disk and creates a checkpoint to recover the database
// from in case of a crash. Writes a checkpoint log with all the ids of active, uncommitted transactions
// to the write-ahead log.
func (rm *RecoveryManager) Checkpoint() error {
	rm.mtx.Lock()
	defer rm.mtx.Unlock()

	for _, table := range rm.db.GetTables() {
		table.GetPager().LockAllPages()
		table.GetPager().FlushAllPages()
		table.GetPager().UnlockAllPages()
	}

	activeIds := make([]uuid.UUID, len(rm.txStack))
	i := 0
	for id := range rm.txStack {
		activeIds[i] = id
		i++
	}

	cl := checkpointLog{
		ids: activeIds,
	}

	err := rm.flushLog(cl)
	if err != nil {
		return err
	}

	rm.delta() // Keep this line at the end that ensures checkpointing works correctly!
	return nil
}

// redo carries out the given table log or edit log's action without
// re-writing the action to the log file. For use when recovering from a crash.
func (rm *RecoveryManager) redo(log log) error {
	switch log := log.(type) {
	case tableLog:
		payload := fmt.Sprintf("create %s table %s", log.tblType, log.tblName)
		_, err := database.HandleCreateTable(rm.db, payload)
		if err != nil {
			return err
		}
	case editLog:
		switch log.action {
		case INSERT_ACTION:
			payload := fmt.Sprintf("insert %v %v into %s", log.key, log.newval, log.tablename)
			err := database.HandleInsert(rm.db, payload)
			if err != nil {
				// There is already an entry, try updating
				payload := fmt.Sprintf("update %s %v %v", log.tablename, log.key, log.newval)
				err = database.HandleUpdate(rm.db, payload)
				if err != nil {
					return err
				}
			}
		case UPDATE_ACTION:
			payload := fmt.Sprintf("update %s %v %v", log.tablename, log.key, log.newval)
			err := database.HandleUpdate(rm.db, payload)
			if err != nil {
				// Entry may have been deleted, try inserting
				payload := fmt.Sprintf("insert %v %v into %s", log.key, log.newval, log.tablename)
				err := database.HandleInsert(rm.db, payload)
				if err != nil {
					return err
				}
			}
		case DELETE_ACTION:
			payload := fmt.Sprintf("delete %v from %s", log.key, log.tablename)
			err := database.HandleDelete(rm.db, payload)
			if err != nil {
				return err
			}
		}
	default:
		return errors.New("can only redo edit or table logs")
	}
	return nil
}

// undo carries out the opposite action of the given edit log's action
// to undo it, returning an error if the undoing action failed.
// Note: writes a log of the undoing action to the log file.
func (rm *RecoveryManager) undo(log editLog) error {
	switch log.action {
	case INSERT_ACTION:
		payload := fmt.Sprintf("delete %v from %s", log.key, log.tablename)
		err := HandleDelete(rm.db, rm.tm, rm, payload, log.id)
		if err != nil {
			return err
		}
	case UPDATE_ACTION:
		payload := fmt.Sprintf("update %s %v %v", log.tablename, log.key, log.oldval)
		err := HandleUpdate(rm.db, rm.tm, rm, payload, log.id)
		if err != nil {
			return err
		}
	case DELETE_ACTION:
		payload := fmt.Sprintf("insert %v %v into %s", log.key, log.oldval, log.tablename)
		err := HandleInsert(rm.db, rm.tm, rm, payload, log.id)
		if err != nil {
			return err
		}
	}
	return nil
}

// Recover carries out a full recovery to the most recent checkpoint according to
// the write-ahead log. Intended to be used on startup after a crash.
func (rm *RecoveryManager) Recover() error {
	logs, checkpointIdx, err := rm.readLogs()
	if err != nil {
		return fmt.Errorf("failed to read logs: %w", err)
	}

	// Track active transactions
	activeTxns := make(map[uuid.UUID]bool)

	// Redo phase: replay operations from last checkpoint
	for i := checkpointIdx; i < len(logs); i++ {
		if err := rm.handleRedoLog(logs[i], activeTxns); err != nil {
			return fmt.Errorf("redo phase failed: %w", err)
		}
	}

	// Begin recovery transactions
	for txnID := range activeTxns {
		rm.tm.Begin(txnID)
	}

	// Undo phase: roll back uncommitted transactions
	if err := rm.performUndoPhase(logs, activeTxns); err != nil {
		return fmt.Errorf("undo phase failed: %w", err)
	}

	return nil
}

func (rm *RecoveryManager) handleRedoLog(log log, activeTxns map[uuid.UUID]bool) error {
	switch l := log.(type) {
	case tableLog, editLog:
		if err := rm.redo(l); err != nil {
			return err
		}
		if el, ok := l.(editLog); ok {
			activeTxns[el.id] = true
		}
	case startLog:
		activeTxns[l.id] = true
	case commitLog:
		delete(activeTxns, l.id)
	}
	return nil
}

func (rm *RecoveryManager) performUndoPhase(logs []log, activeTxns map[uuid.UUID]bool) error {
	for i := len(logs) - 1; i >= 0; i-- {
		switch l := logs[i].(type) {
		case editLog:
			if activeTxns[l.id] {
				if err := rm.undo(l); err != nil {
					return err
				}
			}
		case startLog:
			if activeTxns[l.id] {
				if err := rm.tm.Commit(l.id); err != nil {
					return err
				}
				if err := rm.Commit(l.id); err != nil {
					return err
				}
				delete(activeTxns, l.id)
			}
		}
	}
	return nil
}

// Rollback rolls back the current uncommitted transaction for a client.
// This is called when you abort a transaction.
func (rm *RecoveryManager) Rollback(clientId uuid.UUID) error {
	stack, exists := rm.txStack[clientId]
	if !exists {
		return errors.New("no transaction to rollback")
	}

	for i := len(stack) - 1; i >= 0; i-- {
		err := rm.undo(stack[i])
		if err != nil {
			return err
		}
	}

	rm.Commit(clientId)
	rm.tm.Commit(clientId)
	return nil
}

// Primes the database for recovery
func Prime(folder string) (*database.Database, error) {
	// Ensure folder is of the form */
	base := filepath.Clean(folder)
	recoveryFolder := base + "-recovery/"
	dbFolder := base + "/"

	// If recovery folder doesn't exist, create it and open db folder as normal
	if _, err := os.Stat(recoveryFolder); err != nil {
		if os.IsNotExist(err) {
			err := os.MkdirAll(recoveryFolder, 0775)
			if err != nil {
				return nil, err
			}
			return database.Open(dbFolder)
		}
		return nil, err
	}

	// If recovery folder exists, replace db folder with recovery folder.
	// Copies over log file if it is in the db folder
	logSrcPath := filepath.Join(base, config.LogFileName)
	if _, err := os.Stat(logSrcPath); err == nil {
		logDstPath := filepath.Join(recoveryFolder, config.LogFileName)
		copy.Copy(logSrcPath, logDstPath)
	}
	os.RemoveAll(dbFolder)
	err := copy.Copy(recoveryFolder, dbFolder)
	if err != nil {
		return nil, err
	}
	return database.Open(dbFolder)
}

/////////////////////////////////////////////////////////////////////////////
////////////////////////// Recovery Helper Functions ////////////////////////
/////////////////////////////////////////////////////////////////////////////

// delta copies the entire database to a backup recovery folder.
// Should be called at end of Checkpoint.
func (rm *RecoveryManager) delta() error {
	folder := strings.TrimSuffix(rm.db.GetBasePath(), "/")
	recoveryFolder := folder + "-recovery/"
	folder += "/"
	os.RemoveAll(recoveryFolder)
	err := copy.Copy(folder, recoveryFolder)
	return err
}

// Helper method that gets all log strings and the index of the most recent checkpoint from the log file.
func (rm *RecoveryManager) getRelevantStrings() (
	relevantStrings []string, checkpointPos int, err error) {
	fstats, err := rm.logFile.Stat()
	if err != nil {
		return nil, 0, err
	}

	scanner := backscanner.New(rm.logFile, int(fstats.Size()))
	checkpointTarget := []byte("checkpoint")
	startTarget := []byte("start")
	relevantStrings = make([]string, 0)
	checkpointHit := false
	txs := make(map[uuid.UUID]bool)
	for {
		line, _, err := scanner.LineBytes()
		if err != nil {
			if err == io.EOF {
				return relevantStrings, 0, nil
			} else {
				return nil, 0, err
			}
		}
		relevantStrings = append([]string{string(line)}, relevantStrings...)
		checkpointPos += 1
		if checkpointHit {
			if bytes.Contains(line, startTarget) {
				log, err := logFromString(string(line))
				if err != nil {
					return nil, 0, err
				}
				id := log.(startLog).id
				delete(txs, id)
			}
		}
		if !checkpointHit && bytes.Contains(line, checkpointTarget) {
			checkpointHit = true
			log, err := logFromString(string(line))
			if err != nil {
				return nil, 0, err
			}
			for _, tx := range log.(checkpointLog).ids {
				txs[tx] = true
			}
			checkpointPos = 0
		}
		if checkpointHit && len(txs) <= 0 {
			break
		}
	}
	return relevantStrings, checkpointPos, err
}

// Returns ALL the logs written to disk and the index of the most recent checkpoint log
// (or len(logs) if there were no checkpoint logs).
// Alternatively returns an error if there is an IO or deserialization problem.
func (rm *RecoveryManager) readLogs() (logs []log, checkpointIndex int, err error) {
	strings, checkpointIndex, err := rm.getRelevantStrings()
	if err != nil {
		return nil, 0, err
	}
	if len(strings) > 0 {
		logs = make([]log, len(strings)-1)
		for i, s := range strings[:len(strings)-1] {
			log, err := logFromString(s)
			if err != nil {
				return nil, 0, err
			}
			logs[i] = log
		}
	} else {
		logs = make([]log, 0)
	}
	return logs, checkpointIndex, nil
}
