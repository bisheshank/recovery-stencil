package database

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"dinodb/pkg/entry"
	"dinodb/pkg/repl"
)

// Creates a DB Repl for the given index.
func DatabaseRepl(db *Database) *repl.REPL {
	r := repl.NewRepl()
	r.AddCommand("create", func(payload string, replConfig *repl.REPLConfig) (string, error) {
		return HandleCreateTable(db, payload)
	}, "Create a table. usage: create <btree|hash> table <table>")

	r.AddCommand("find", func(payload string, replConfig *repl.REPLConfig) (string, error) {
		return HandleFind(db, payload)
	}, "Find an element. usage: find <key> from <table>")

	r.AddCommand("insert", func(payload string, replConfig *repl.REPLConfig) (string, error) {
		return "", HandleInsert(db, payload)
	}, "Insert an element. usage: insert <key> <value> into <table>")

	r.AddCommand("update", func(payload string, replConfig *repl.REPLConfig) (string, error) {
		return "", HandleUpdate(db, payload)
	}, "Update en element. usage: update <table> <key> <value>")

	r.AddCommand("delete", func(payload string, replConfig *repl.REPLConfig) (string, error) {
		return "", HandleDelete(db, payload)
	}, "Delete an element. usage: delete <key> from <table>")

	r.AddCommand("select", func(payload string, replConfig *repl.REPLConfig) (string, error) {
		return HandleSelect(db, payload)
	}, "Select elements from a table. usage: select from <table>")

	r.AddCommand("pretty", func(payload string, replConfig *repl.REPLConfig) (string, error) {
		return HandlePretty(db, payload)
	}, "Print out the internal data representation. usage: pretty")

	return r
}

// Handle create table.
func HandleCreateTable(d *Database, payload string) (output string, err error) {
	fields := strings.Fields(payload)
	numFields := len(fields)
	// Usage: create <type> table <table>
	if numFields != 4 || fields[2] != "table" || (fields[1] != "btree" && fields[1] != "hash") {
		return "", fmt.Errorf("usage: create <btree|hash> table <table>")
	}
	var tableType IndexType
	switch fields[1] {
	case "btree":
		tableType = BTreeIndexType
	case "hash":
		tableType = HashIndexType
	default:
		return "", errors.New("create error: internal error")
	}
	tableName := fields[3]
	_, err = d.CreateTable(tableName, tableType)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%s table %s created.\n", fields[1], tableName), nil
}

// Handle find.
func HandleFind(d *Database, payload string) (output string, err error) {
	fields := strings.Fields(payload)
	numFields := len(fields)
	// Usage: find <key> from <table>
	var key int
	if numFields != 4 || fields[2] != "from" {
		return "", fmt.Errorf("usage: find <key> from <table>")
	}
	if key, err = strconv.Atoi(fields[1]); err != nil {
		return "", fmt.Errorf("find error: %v", err)
	}
	tableName := fields[3]
	table, err := d.GetTable(tableName)
	if err != nil {
		return "", fmt.Errorf("find error: %v", err)
	}
	entry, err := table.Find(int64(key))
	if err != nil {
		return "", fmt.Errorf("find error: %v", err)
	}

	return fmt.Sprintf("found entry: (%d, %d)\n", entry.Key, entry.Value), nil
}

// Handle insert.
func HandleInsert(d *Database, payload string) (err error) {
	fields := strings.Fields(payload)
	numFields := len(fields)
	// Usage: insert <key> <value> into <table>
	var key, value int
	if numFields != 5 || fields[3] != "into" {
		return fmt.Errorf("usage: insert <key> <value> into <table>")
	}
	if key, err = strconv.Atoi(fields[1]); err != nil {
		return fmt.Errorf("insert error: %v", err)
	}
	if value, err = strconv.Atoi(fields[2]); err != nil {
		return fmt.Errorf("insert error: %v", err)
	}
	tableName := fields[4]
	table, err := d.GetTable(tableName)
	if err != nil {
		return fmt.Errorf("insert error: %v", err)
	}
	_, err = table.Find(int64(key))
	if err == nil {
		return fmt.Errorf("insert error: key already in table")
	}
	err = table.Insert(int64(key), int64(value))
	if err != nil {
		return fmt.Errorf("insert error: %v", err)
	}
	return nil
}

// Handle update.
func HandleUpdate(d *Database, payload string) (err error) {
	fields := strings.Fields(payload)
	numFields := len(fields)
	// Usage: update <table> <key> <value>
	var key, value int
	if numFields != 4 {
		return fmt.Errorf("usage: update <table> <key> <value>")
	}
	if key, err = strconv.Atoi(fields[2]); err != nil {
		return fmt.Errorf("update error: %v", err)
	}
	if value, err = strconv.Atoi(fields[3]); err != nil {
		return fmt.Errorf("update error: %v", err)
	}
	tableName := fields[1]
	table, err := d.GetTable(tableName)
	if err != nil {
		return fmt.Errorf("update error: %v", err)
	}
	err = table.Update(int64(key), int64(value))
	if err != nil {
		return fmt.Errorf("update error: %v", err)
	}
	return nil
}

// Handle delete.
func HandleDelete(d *Database, payload string) (err error) {
	fields := strings.Fields(payload)
	numFields := len(fields)
	// Usage: delete <key> from <table>
	var key int
	if numFields != 4 || fields[2] != "from" {
		return fmt.Errorf("usage: delete <key> from <table>")
	}
	if key, err = strconv.Atoi(fields[1]); err != nil {
		return fmt.Errorf("delete error: %v", err)
	}
	tableName := fields[3]
	table, err := d.GetTable(tableName)
	if err != nil {
		return fmt.Errorf("delete error: %v", err)
	}
	err = table.Delete(int64(key))
	if err != nil {
		return fmt.Errorf("delete error: %v", err)
	}
	return nil
}

// Handle select.
func HandleSelect(d *Database, payload string) (output string, err error) {
	fields := strings.Fields(payload)
	numFields := len(fields)
	w := new(strings.Builder)
	// Usage: select from <table>
	if numFields != 3 || fields[1] != "from" {
		return "", fmt.Errorf("usage: select from <table>")
	}
	tableName := fields[2]
	table, err := d.GetTable(tableName)
	if err != nil {
		return "", fmt.Errorf("select error: %v", err)
	}
	var results []entry.Entry
	if results, err = table.Select(); err != nil {
		return "", err
	}
	printResults(results, w)
	return w.String(), nil
}

// Handle pretty printing.
func HandlePretty(d *Database, payload string) (output string, err error) {
	fields := strings.Fields(payload)
	numFields := len(fields)
	w := new(strings.Builder)
	// Usage: pretty <optional pagenumber> from <table>
	if numFields == 3 && fields[1] == "from" {
		tableName := fields[2]
		table, err := d.GetTable(tableName)
		if err != nil {
			return "", fmt.Errorf("pretty error: %v", err)
		}
		table.Print(w)
	} else if numFields == 4 && fields[2] == "from" {
		var pn int
		if pn, err = strconv.Atoi(fields[1]); err != nil {
			return "", fmt.Errorf("pretty error: %v", err)
		}
		tableName := fields[3]
		table, err := d.GetTable(tableName)
		if err != nil {
			return "", fmt.Errorf("pretty error: %v", err)
		}
		table.PrintPN(pn, w)
	} else {
		return "", fmt.Errorf("usage: pretty <optional pagenumber> from <table>")
	}
	return w.String(), nil
}

// printResults prints all given entries in a standard format.
func printResults(entries []entry.Entry, w io.Writer) {
	for _, entry := range entries {
		io.WriteString(w, fmt.Sprintf("(%v, %v)\n",
			entry.Key, entry.Value))
	}
}
