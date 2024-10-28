package hash

import (
	"io"
	"path/filepath"

	"dinodb/pkg/entry"
	"dinodb/pkg/pager"
)

// HashIndex is an index that uses a HashTable as its underlying datastructure.
type HashIndex struct {
	table *HashTable   // The HashTable
	pager *pager.Pager // The pager backing this index / HashTable
}

// Opens the pager with the given table name.
func OpenTable(filename string) (*HashIndex, error) {
	// Create a pager for the table.
	pager, err := pager.New(filename)
	if err != nil {
		return nil, err
	}
	// Return index.
	var table *HashTable
	if pager.GetNumPages() == 0 {
		table, err = NewHashTable(pager)
	} else {
		table, err = ReadHashTable(pager)
	}
	if err != nil {
		return nil, err
	}
	return &HashIndex{table: table, pager: pager}, nil
}

// GetName returns the base file name of the file backing this index's pager.
func (table *HashIndex) GetName() string {
	return filepath.Base(table.pager.GetFileName())
}

// GetPager returns the pager backing this index
func (table *HashIndex) GetPager() *pager.Pager {
	return table.pager
}

// Get table.
func (index *HashIndex) GetTable() *HashTable {
	return index.table
}

// Closes the table by closing the pager.
func (index *HashIndex) Close() error {
	return WriteHashTable(index.pager, index.table)
}

// Find element by key.
func (index *HashIndex) Find(key int64) (entry.Entry, error) {
	return index.table.Find(key)
}

// Insert given element.
func (index *HashIndex) Insert(key int64, value int64) error {
	return index.table.Insert(key, value)
}

// Update given element.
func (index *HashIndex) Update(key int64, value int64) error {
	return index.table.Update(key, value)
}

// Delete given element.
func (index *HashIndex) Delete(key int64) error {
	return index.table.Delete(key)
}

// Select all elements.
func (index *HashIndex) Select() ([]entry.Entry, error) {
	return index.table.Select()
}

// Print all elements.
func (index *HashIndex) Print(w io.Writer) {
	index.table.Print(w)
}

// Print a page of elements.
func (index *HashIndex) PrintPN(pn int, w io.Writer) {
	index.table.PrintPN(pn, w)
}
