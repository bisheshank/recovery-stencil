package btree

import (
	"dinodb/pkg/entry"
	"dinodb/pkg/pager"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
)

// LeafNode represents a node at the bottom of a B+Tree that stores the actual
// key and value pairs that represent our data.
type LeafNode struct {
	NodeHeader           // Embeds all NodeHeader fields.
	rightSiblingPN int64 // The page number of the right sibling node.
	parent         Node  // A pointer to the parent node (only used in CONCURRENCY for unlocking).
}

// insert finds the appropriate place in the leaf node to insert a new key-value pair.
// If an entry already exists with the specified key, return an error. Returns a Split with
// relevant data to be used  by the caller if the insertion results in the node splitting.
//
// If the update flag is true, then insert will update the value of an existing key instead,
// returning an error if an existing entry to overwrite is not found.
// CONCURRENCY: 
// - Unlock parents if it is impossible to split
// - The insert should fully complete at the leaf node, so make sure to unlock accordingly
func (node *LeafNode) insert(key int64, value int64, update bool) (Split, error) {
	/* SOLUTION {{{ */
	// [CONCURRENCY] Unlock parents if it is impossible to split in this operation
	if !node.canSplit() {
		node.unlockParents()
	}
	defer node.unlock()
	// Get insert position.
	insertPos := node.search(key)
	// Check if this is a duplicate entry.
	if insertPos < node.numKeys && node.getKeyAt(insertPos) == key {
		defer node.unlockParents()
		if update {
			node.updateValueAt(insertPos, value)
			return Split{}, nil
		} else {
			return Split{}, errors.New("cannot insert duplicate key")
		}
	}
	// Return an error if we're updating a non-existent entry.
	if update {
		node.unlockParents()
		return Split{}, errors.New("cannot update non-existent entry")
	}
	// Shift entries to the right if needed.
	for i := node.numKeys - 1; i >= insertPos; i-- {
		node.updateKeyAt(i+1, node.getKeyAt(i))
		node.updateValueAt(i+1, node.getValueAt(i))
	}
	node.updateNumKeys(node.numKeys + 1)
	// Modify the Entry at this position.
	node.modifyEntry(insertPos, entry.New(key, value))
	// Check if we need to split the node.
	if node.numKeys >= ENTRIES_PER_LEAF_NODE {
		// Note we are not unlocking parents here
		return node.split()
	}
	node.unlockParents()
	return Split{}, nil
	/* SOLUTION }}} */
}

// split is a helper function to split a leaf node, then propagate the split upwards.
func (node *LeafNode) split() (Split, error) {
	/* SOLUTION {{{ */
	// Create a new leaf node to split our keys.
	pager := node.page.GetPager()
	newNode, err := createLeafNode(pager)
	if err != nil {
		return Split{}, err
	}
	defer pager.PutPage(newNode.getPage())
	// Set the right sibling for our two nodes.
	prevSiblingPN := node.setRightSibling(newNode.page.GetPageNum())
	newNode.setRightSibling(prevSiblingPN)
	// Transfer entries to the new node (plus the new entry) accordingly.
	midpoint := node.numKeys / 2
	for i := midpoint; i < node.numKeys; i++ {
		newNode.updateKeyAt(newNode.numKeys, node.getKeyAt(i))
		newNode.updateValueAt(newNode.numKeys, node.getValueAt(i))
		newNode.updateNumKeys(newNode.numKeys + 1)
	}
	node.updateNumKeys(midpoint)
	return Split{
		isSplit: true,
		key:     newNode.getKeyAt(0), // Get the right node's first key (median before split)
		leftPN:  node.page.GetPageNum(),
		rightPN: newNode.page.GetPageNum(),
	}, nil
	/* SOLUTION }}} */
}

// delete removes a given key-value pair from the leaf node, if the given key exists.
func (node *LeafNode) delete(key int64) {
	// [CONCURRENCY] Unlock parents, eventually unlock this node
	node.unlockParents()
	defer node.unlock()
	// Find index of the specified key
	deletePos := node.search(key)
	if deletePos >= node.numKeys || node.getKeyAt(deletePos) != key {
		// Key was not found, so nothing to delete
		return
	}
	// Shift entries to the left, overwriting the key-value pair to be deleted
	for i := deletePos; i < node.numKeys-1; i++ {
		node.updateKeyAt(i, node.getKeyAt(i+1))
		node.updateValueAt(i, node.getValueAt(i+1))
	}
	node.updateNumKeys(node.numKeys - 1)
}

// get returns a boolean indicating whether the specified key was found,
// and if it was found, also returns the key's associated value.
func (node *LeafNode) get(key int64) (value int64, found bool) {
	// [CONCURRENCY] Unlock parents and eventually unlock this node
	node.unlockParents()
	defer node.unlock()
	// Find index of key
	index := node.search(key)
	if index >= node.numKeys || node.getKeyAt(index) != key {
		// Key was not found, so return false
		return 0, false
	}
	entry := node.getEntry(index)
	return entry.Value, true
}

/////////////////////////////////////////////////////////////////////////////
////////////////////////// Leaf Node  Helper Functions //////////////////////
/////////////////////////////////////////////////////////////////////////////

// search returns the first index where key >= given key.
// If no key satisfies this condition, returns numKeys.
func (node *LeafNode) search(key int64) int64 {
	// Binary search for the key.
	minIndex := sort.Search(
		int(node.numKeys),
		func(idx int) bool {
			return node.getKeyAt(int64(idx)) >= key
		},
	)
	return int64(minIndex)
}

// printNode pretty prints our leaf node.
func (node *LeafNode) printNode(w io.Writer, firstPrefix string, prefix string) {
	// Format header data.
	var nodeType string = "Leaf"
	var isRoot string
	if node.isRoot() {
		isRoot = " (root)"
	}
	numKeys := strconv.Itoa(int(node.numKeys))
	// Print header data.
	io.WriteString(w, fmt.Sprintf("%v[%v] %v%v size: %v\n",
		firstPrefix, node.page.GetPageNum(), nodeType, isRoot, numKeys))
	// Print entries.
	for Entrynum := int64(0); Entrynum < node.numKeys; Entrynum++ {
		entry := node.getEntry(Entrynum)
		io.WriteString(w, fmt.Sprintf("%v |--> (%v, %v)\n",
			prefix, entry.Key, entry.Value))
	}
	if node.rightSiblingPN > 0 {
		io.WriteString(w, fmt.Sprintf("%v |--+\n", prefix))
		io.WriteString(w, fmt.Sprintf("%v    | right sibling @ [%v]\n",
			prefix, node.rightSiblingPN))
		io.WriteString(w, fmt.Sprintf("%v    v\n", prefix))
	}
}

// pageToLeafNode returns the leaf node that is stored in the specified page.
// Concurrency note: the given page must at least be read-locked before calling.
func pageToLeafNode(page *pager.Page) *LeafNode {
	nodeHeader := pageToNodeHeader(page)
	rightSiblingPN, _ := binary.Varint(
		page.GetData()[RIGHT_SIBLING_PN_OFFSET : RIGHT_SIBLING_PN_OFFSET+RIGHT_SIBLING_PN_SIZE],
	)
	return &LeafNode{
		nodeHeader,
		rightSiblingPN,
		nil,
	}
}

// createLeafNode creates and returns a new, empty leaf node.
// Nodes created with this function must use `PutPage()` accordingly after use.
func createLeafNode(pager *pager.Pager) (*LeafNode, error) {
	newPage, err := pager.GetNewPage()
	if err != nil {
		return &LeafNode{}, err
	}
	// Don't need to lock newPage here since we are the only one who can have a reference to it
	initPage(newPage, LEAF_NODE)
	return pageToLeafNode(newPage), nil
}

// getPage returns a pointer to the leaf node's page.
func (node *LeafNode) getPage() *pager.Page {
	return node.page
}

// getNodeType returns leafNode.
func (node *LeafNode) getNodeType() NodeType {
	return node.nodeType
}

// copy copies the metadata and data of the passed in LeafNode to this LeafNode.
// Concurrency note: the toCopy node's page must at least be read-locked before calling.
func (node *LeafNode) copy(toCopy *LeafNode) {
	node.page.Update(toCopy.page.GetData(), 0, pager.Pagesize)
	node.updateNumKeys(toCopy.numKeys)
	node.setRightSibling(toCopy.rightSiblingPN)
}

// isRoot returns true if the current node is the root node.
func (node *LeafNode) isRoot() bool {
	return node.page.GetPageNum() == ROOT_PN
}

// setRightSibling sets the right sibling pagenumber field of the leaf node
// and updates the leaf node's page accordingly. Returns the old right sibling.
func (node *LeafNode) setRightSibling(siblingPN int64) int64 {
	// Retrieve the old sibling data
	oldSiblingPN := node.rightSiblingPN
	// Write the new sibling data to the page
	node.rightSiblingPN = siblingPN
	siblingData := make([]byte, RIGHT_SIBLING_PN_SIZE)
	binary.PutVarint(siblingData, node.rightSiblingPN)
	node.page.Update(
		siblingData,
		RIGHT_SIBLING_PN_OFFSET,
		RIGHT_SIBLING_PN_SIZE,
	)
	return oldSiblingPN
}

// entryPos returns the page offset to the entry at the given index.
func (node *LeafNode) entryPos(index int64) int64 {
	return LEAF_NODE_HEADER_SIZE + index*ENTRYSIZE
}

// modifyEntry updates the data stored in the entry at the given index.
func (node *LeafNode) modifyEntry(index int64, entry entry.Entry) {
	newdata := entry.Marshal()
	startPos := node.entryPos(index)
	node.page.Update(newdata, startPos, ENTRYSIZE)
}

// getEntry returns the entry stored in the entry at the given index.
// Concurrency note: this LeafNode must at least be read-locked before calling.
func (node *LeafNode) getEntry(index int64) entry.Entry {
	startPos := node.entryPos(index)
	// Deserialize the entry.
	entry := entry.UnmarshalEntry(node.page.GetData()[startPos : startPos+ENTRYSIZE])
	return entry
}

// getKeyAt returns the key stored at the given index of the leaf node.
// Concurrency note: this LeafNode must at least be read-locked before calling.
func (node *LeafNode) getKeyAt(index int64) int64 {
	return node.getEntry(index).Key
}

// updateKeyAt updates the key at the given index of the leaf node.
func (node *LeafNode) updateKeyAt(index int64, newKey int64) {
	existingVal := node.getValueAt(index)
	node.modifyEntry(index, entry.New(newKey, existingVal))
}

// getValueAt returns the value stored at the given index of the leaf node.
// Concurrency note: this LeafNode must at least be read-locked before calling.
func (node *LeafNode) getValueAt(index int64) int64 {
	return node.getEntry(index).Value
}

// updateValueAt updates the value at the given index of the leaf node.
func (node *LeafNode) updateValueAt(index int64, newVal int64) {
	existingKey := node.getKeyAt(index)
	node.modifyEntry(index, entry.New(existingKey, newVal))
}

// updateNumKeys updates the numKeys field in the node struct and the page.
func (node *LeafNode) updateNumKeys(newNumKeys int64) {
	node.numKeys = newNumKeys
	// Write the new data to the page
	nKeysData := make([]byte, NUM_KEYS_SIZE)
	binary.PutVarint(nKeysData, newNumKeys)
	node.page.Update(nKeysData, NUM_KEYS_OFFSET, NUM_KEYS_SIZE)
}

/////////////////////////////////////////////////////////////////////////////
////////////////////////// Lock  Helper Functions ///////////////////////////
/////////////////////////////////////////////////////////////////////////////

// canSplit returns whether this node has the capability to split in the next insert operation.
func (node *LeafNode) canSplit() bool {
	return node.numKeys == ENTRIES_PER_LEAF_NODE-1
}

// unlockParents unlocks all of this node's locked parents.
func (node *LeafNode) unlockParents() {
	// Remove this node's parent pointer
	parent := node.parent
	node.parent = nil
	// Parent pointers are only set if the node's parent is locked -
	// take advantage of this to iteratively unlock all locked parents
	for parent != nil {
		switch castedParent := parent.(type) {
		case *InternalNode:
			parent = castedParent.parent
			castedParent.unlock()
		case *LeafNode:
			panic("Should never have a leaf as a parent")
		}
	}
}

// unlock unlocks this leaf node.
func (node *LeafNode) unlock() {
	node.parent = nil
	node.page.WUnlock()
}
