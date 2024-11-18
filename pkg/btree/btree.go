package btree

import (
	"errors"
	"fmt"
	"io"
	"path/filepath"

	"dinodb/pkg/entry"
	"dinodb/pkg/pager"
)

// BTreeIndex is an index that uses a B+Tree as it's underlying data structure
type BTreeIndex struct {
	pager  *pager.Pager // The pager used to store the B+Tree's data.
	rootPN int64        // The pagenum of this B+Tree's root node.
}

// OpenIndex returns a BTreeIndex that stores its data in a file with the given name.
// If the file doesn't exist or is empty, creates and returns a BTreeIndex with an empty B+Tree.
func OpenIndex(filename string) (*BTreeIndex, error) {
	// Create a pager for the B+Tree
	pager, err := pager.New(filename)
	if err != nil {
		return nil, err
	}
	// Initialize the pager if it's new, creating a leaf root node
	if pager.GetNumPages() == 0 {
		rootPage, err := pager.GetNewPage()
		if err != nil {
			return nil, err
		}
		defer pager.PutPage(rootPage)
		initPage(rootPage, LEAF_NODE)
		rootNode := pageToLeafNode(rootPage)
		rootNode.setRightSibling(-1)
	}
	return &BTreeIndex{pager: pager, rootPN: ROOT_PN}, nil
}

// GetName returns the base file name of the file backing this index's pager.
func (index *BTreeIndex) GetName() string {
	return filepath.Base(index.pager.GetFileName())
}

// Get this index's pager.
func (index *BTreeIndex) GetPager() *pager.Pager {
	return index.pager
}

// Close flushes all changes to disk.
func (index *BTreeIndex) Close() (err error) {
	err = index.pager.Close()
	return err
}

// Find returns the entry associated with the given key, or an error if
// no entry with that key is found.
func (index *BTreeIndex) Find(key int64) (entry.Entry, error) {
	// Get the root node.
	rootPage, err := index.pager.GetPage(index.rootPN)
	if err != nil {
		return entry.Entry{}, err
	}
	// [CONCURRENCY] Lock and eventually unlock the root node.
	lockRoot(rootPage)
	rootNode := pageToNode(rootPage)
	initRootNode(rootNode)
	defer unsafeUnlockRoot(rootNode)
	defer index.pager.PutPage(rootPage)
	// Start the lookup process on the root node
	value, found := rootNode.get(key)
	if found {
		return entry.New(key, value), nil
	}
	return entry.Entry{}, fmt.Errorf("no entry with key %d was found", key)
}

// Insert inserts a key-value entry into the B+Tree,
// returning an error if there is a problem with the insertion or splitting process.
func (index *BTreeIndex) Insert(key int64, value int64) error {
	// Get the root node.
	rootPage, err := index.pager.GetPage(index.rootPN)
	if err != nil {
		return err
	}
	// [CONCURRENCY] Lock and eventually unlock the root node.
	lockRoot(rootPage)
	rootNode := pageToNode(rootPage)
	initRootNode(rootNode)
	defer unsafeUnlockRoot(rootNode)
	defer index.pager.PutPage(rootPage)
	// Insert the entry into the root node.
	result, err := rootNode.insert(key, value, false)
	if err != nil || !result.isSplit {
		return err
	}
	// Split the root node.
	// Remember to preserve the invariant that the root node occupies page 0.
	// [CONCURRENCY]
	// Unlock the super node. This is necessary because if the root node split,
	// Then it will have called unlock() on itself, but will not have called
	// unlockParents(). In unlock(), a node sets its parent to nil, so then
	// unsafeUnlockRoot() will not catch this either. As such, manually unlock it.
	defer SUPER_NODE.unlock()
	// Ensure that our left PN hasn't changed.
	if result.leftPN != 0 {
		return errors.New("splitting was corrupted")
	}
	// Create a new node to transfer our data.
	var newNodePN int64
	// Depending on whether the root is a leaf or an internal node...
	if rootNode.getNodeType() == LEAF_NODE {
		// Create a new leaf node.
		newNode, err := createLeafNode(index.pager)
		if err != nil {
			return errors.New("failed to split root node")
		}
		defer index.pager.PutPage(newNode.page)
		// Copy the attributes from the root node.
		leafyRoot := pageToLeafNode(rootNode.getPage())
		newNode.copy(leafyRoot)
		newNodePN = newNode.page.GetPageNum()
	} else {
		// Create a new internal node.
		newNode, err := createInternalNode(index.pager)
		if err != nil {
			return errors.New("failed to split root node")
		}
		defer index.pager.PutPage(newNode.page)
		// Copy the attributes from the root node.
		internedRoot := pageToInternalNode(rootNode.getPage())
		newNode.copy(internedRoot)
		newNodePN = newNode.page.GetPageNum()
	}
	// Reinitialize the root node.
	initPage(rootNode.getPage(), INTERNAL_NODE)
	newRoot := pageToInternalNode(rootNode.getPage())
	// Populate the pointers to children.
	newRoot.updateKeyAt(0, result.key)
	newRoot.updatePNAt(0, newNodePN)
	newRoot.updatePNAt(1, result.rightPN)
	newRoot.updateNumKeys(1)
	return nil
}

// Update modifies the value associated with an existing key.
func (index *BTreeIndex) Update(key int64, value int64) error {
	// Get the root node.
	rootPage, err := index.pager.GetPage(index.rootPN)
	if err != nil {
		return err
	}
	// [CONCURRENCY] Lock and eventually unlock the root node.
	lockRoot(rootPage)
	rootNode := pageToNode(rootPage)
	initRootNode(rootNode)
	defer unsafeUnlockRoot(rootNode)
	defer index.pager.PutPage(rootPage)
	// Update the entry.
	_, err = rootNode.insert(key, value, true)
	return err
}

// Delete removes the entry with the given key from the B+Tree.
func (index *BTreeIndex) Delete(key int64) error {
	// Get the root node.
	rootPage, err := index.pager.GetPage(index.rootPN)
	if err != nil {
		return err
	}
	// [CONCURRENCY] Lock and eventually unlock the root node.
	lockRoot(rootPage)
	rootNode := pageToNode(rootPage)
	initRootNode(rootNode)
	defer unsafeUnlockRoot(rootNode)
	defer index.pager.PutPage(rootPage)
	// Delete the key.
	rootNode.delete(key)
	return nil
}

// Select returns a slice of all the entries in the B+Tree
// ordered by their keys.
func (index *BTreeIndex) Select() ([]entry.Entry, error) {
	/* SOLUTION {{{ */
	// Use a cursor to traverse the B+Tree from start to end
	entries := make([]entry.Entry, 0)
	// Get a cursor pointing to the first entry
	// Cursor returns locked
	cursor, err := index.CursorAtStart()
	if err != nil {
		return nil, err
	}
	defer cursor.Close()

	// Traverse over all entries.
	for {
		entry, err := cursor.GetEntry()
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
		if cursor.Next() {
			break
		}
	}

	return entries, nil
	/* SOLUTION }}} */
}

// SelectRange returns a slice of entries with keys between the startKey and endKey.
// startKey is inclusive, and endKey is exclusive --> [startKey, endKey).
// return an error if startKey >= endKey or some other error occurs
func (index *BTreeIndex) SelectRange(startKey int64, endKey int64) ([]entry.Entry, error) {
	/* SOLUTION {{{ */
	if startKey >= endKey {
		return nil, errors.New("startKey is not smaller than endKey")
	}
	ret := make([]entry.Entry, 0)
	c, err := index.CursorAt(startKey)
	defer c.Close()
	if err != nil {
		return nil, err
	}
	// Get the first entry that the cursor is pointing at
	checkEntry, err := c.GetEntry()
	if err != nil {
		return nil, err
	}
	// Get all the desired entries by looping until endKey is reached/exceeded
	// or until we don't have any more entries
	for endKey > checkEntry.Key {
		ret = append(ret, checkEntry)
		if c.Next() {
			return ret, nil
		}
		checkEntry, err = c.GetEntry()
		if err != nil {
			return ret, nil
		}
	}
	return ret, nil
	/* SOLUTION }}} */
}

// Print will pretty-print all nodes in the B+Tree.
func (index *BTreeIndex) Print(w io.Writer) {
	rootPage, err := index.pager.GetPage(index.rootPN)
	if err != nil {
		return
	}
	defer index.pager.PutPage(rootPage)
	rootNode := pageToNode(rootPage)
	rootNode.printNode(w, "", "")
}

// PrintPN will pretty-print the node with page number PN.
func (index *BTreeIndex) PrintPN(pagenum int, w io.Writer) {
	page, err := index.pager.GetPage(int64(pagenum))
	if err != nil {
		return
	}
	defer index.pager.PutPage(page)
	node := pageToNode(page)
	node.printNode(w, "", "")
}
