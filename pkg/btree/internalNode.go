package btree

import (
	"dinodb/pkg/pager"
	"encoding/binary"
	"fmt"
	"io"
	"sort"
	"strconv"
)

// InternalNode represents a non-leaf node in our B+Tree that stores search keys
// and pointers to child nodes to aid traversal.
type InternalNode struct {
	NodeHeader      // Embeds all NodeHeader fields.
	parent     Node // A pointer to the parent node (only used in CONCURRENCY for unlocking)
}

// insert finds the appropriate place in a leaf node to insert a new tuple.
// [CONCURRENCY] 
// - Unlock parents if it is impossible to split in this operation
// - Continue with hand-over-hand locking with child node
func (node *InternalNode) insert(key int64, value int64, update bool) (Split, error) {
	// [CONCURRENCY] Unlock parents if it is impossible to split in this operation
	if !node.canSplit() {
		node.unlockParents()
	}
	// Insert the entry into the appropriate child node.
	childIdx := node.search(key)
	child, childErr := node.getAndLockChildAt(childIdx)
	if childErr != nil {
		return Split{}, childErr
	}
	// [CONCURRENCY]
	node.initChild(child)

	pager := child.getPage().GetPager()
	defer pager.PutPage(child.getPage())
	// Insert value into the child.
	result, childErr := child.insert(key, value, update)
	if childErr != nil {
		return Split{}, childErr
	}
	// Insert a new key into our node if necessary.
	if result.isSplit {
		split, insertSplitErr := node.insertSplit(result)
		defer node.unlock()
		if !split.isSplit {
			node.unlockParents()
		}
		return split, insertSplitErr
	}
	// This is the case when there was no split and no child err
	return Split{}, nil
}

// insertSplit inserts a split result into an internal node.
// If this insertion results in another split, the split is cascaded upwards.
func (node *InternalNode) insertSplit(split Split) (Split, error) {
	/* SOLUTION {{{ */
	insertPos := node.search(split.key)
	// Shift keys to the right.
	for i := node.numKeys - 1; i >= insertPos; i-- {
		node.updateKeyAt(i+1, node.getKeyAt(i))
	}
	// Shift children to the right.
	for i := node.numKeys; i > insertPos; i-- {
		node.updatePNAt(i+1, node.getPNAt(i))
	}
	// Insert the new key and pagenumber at this position.
	node.updateKeyAt(insertPos, split.key)
	node.updatePNAt(insertPos+1, split.rightPN)
	node.updateNumKeys(node.numKeys + 1)
	// Check if we need to split.
	if node.numKeys >= KEYS_PER_INTERNAL_NODE {
		return node.split()
	}
	return Split{}, nil
	/* SOLUTION }}} */
}

// split is a helper function that splits an internal node, then propagates the split upwards.
func (node *InternalNode) split() (Split, error) {
	/* SOLUTION {{{ */
	// Create a new internal node to move half our keys to
	newNode, err := createInternalNode(node.page.GetPager())
	if err != nil {
		return Split{}, err
	}
	pager := newNode.getPage().GetPager()
	defer pager.PutPage(newNode.getPage())
	// Compute the midpoint index based on the number of children to move
	midpoint := (node.numKeys - 1) / 2
	// Transfer the keys to the right of the midpoint to the new node.
	for i := midpoint + 1; i < node.numKeys; i++ {
		newNode.updatePNAt(newNode.numKeys, node.getPNAt(i))
		newNode.updateKeyAt(newNode.numKeys, node.getKeyAt(i))
		newNode.updateNumKeys(newNode.numKeys + 1)
	}
	newNode.updatePNAt(newNode.numKeys, node.getPNAt(node.numKeys))

	middleKey := node.getKeyAt(midpoint)
	node.updateNumKeys(midpoint)
	// Propagate the split.
	return Split{
		isSplit: true,
		key:     middleKey,
		leftPN:  node.page.GetPageNum(),
		rightPN: newNode.page.GetPageNum(),
	}, nil
	/* SOLUTION }}} */
}

// delete removes a given tuple from the leaf node, if the given key exists.
func (node *InternalNode) delete(key int64) {
	// [CONCURRENCY] Unlock all parent nodes
	node.unlockParents()
	// Get the next child node where the key would be located under
	childIdx := node.search(key)
	child, err := node.getAndLockChildAt(childIdx)
	if err != nil {
		return
	}
	// [CONCURRENCY] initialize child node's parent pointer
	node.initChild(child)
	pager := child.getPage().GetPager()
	defer pager.PutPage(child.getPage())
	// Delete from child
	child.delete(key)
}

// get returns the value associated with a given key from the leaf node.
func (node *InternalNode) get(key int64) (value int64, found bool) {
	// [CONCURRENCY] Unlock parents.
	node.unlockParents()
	// Find the child.
	childIdx := node.search(key)
	child, err := node.getAndLockChildAt(childIdx)
	if err != nil {
		return 0, false
	}
	// [CONCURRENCY] initialize child's parent pointer
	node.initChild(child)
	pager := child.getPage().GetPager()
	defer pager.PutPage(child.getPage())
	return child.get(key)
}

/////////////////////////////////////////////////////////////////////////////
///////////////////// Internal Node  Helper Functions ///////////////////////
/////////////////////////////////////////////////////////////////////////////

// search returns the first index where key > given key.
// If no such index exists, it returns numKeys.
func (node *InternalNode) search(key int64) int64 {
	// Binary search for the key.
	minIndex := sort.Search(
		int(node.numKeys),
		func(idx int) bool {
			return node.getKeyAt(int64(idx)) > key
		},
	)
	return int64(minIndex)
}

// printNode pretty prints our internal node.
func (node *InternalNode) printNode(w io.Writer, firstPrefix string, prefix string) {
	// Format header data.
	var nodeType string = "Internal"
	var isRoot string
	if node.isRoot() {
		isRoot = " (root)"
	}
	numKeys := strconv.Itoa(int(node.numKeys + 1))
	// Print header data.
	io.WriteString(w, fmt.Sprintf("%v[%v] %v%v size: %v\n",
		firstPrefix, node.page.GetPageNum(), nodeType, isRoot, numKeys))
	// Print entries.
	nextFirstPrefix := prefix + " |--> "
	nextPrefix := prefix + " |    "
	for idx := int64(0); idx <= node.numKeys; idx++ {
		io.WriteString(w, fmt.Sprintf("%v\n", nextPrefix))
		child, err := node.getChildAt(idx)
		if err != nil {
			return
		}
		pager := child.getPage().GetPager()
		defer pager.PutPage(child.getPage())
		child.printNode(w, nextFirstPrefix, nextPrefix)
		if idx != node.numKeys {
			io.WriteString(w, fmt.Sprintf("\n%v[KEY] %v\n", nextPrefix, node.getKeyAt(idx)))
		}
	}
}

// pageToInternalNode returns the internal node corresponding to the given page.
// Concurrency note: the given page must at least be read-locked before calling.
func pageToInternalNode(page *pager.Page) *InternalNode {
	nodeHeader := pageToNodeHeader(page)
	return &InternalNode{nodeHeader, nil}
}

// createInternalNode creates and returns a new internal node.
// Nodes created with this function must use `PutPage()` accordingly after use.
func createInternalNode(pager *pager.Pager) (*InternalNode, error) {
	newPage, err := pager.GetNewPage()
	if err != nil {
		return &InternalNode{}, err
	}
	initPage(newPage, INTERNAL_NODE)
	return pageToInternalNode(newPage), nil
}

// getPage returns the internal node's page.
func (node *InternalNode) getPage() *pager.Page {
	return node.page
}

// getNodeType returns internalNode.
func (node *InternalNode) getNodeType() NodeType {
	return node.nodeType
}

// copy copies the metadata and data of the passed in InternalNode to this InternalNode.
// Concurrency note: the toCopy node's page must at least be read-locked before calling.
func (node *InternalNode) copy(toCopy *InternalNode) {
	node.page.Update(toCopy.page.GetData(), 0, pager.Pagesize)
	node.updateNumKeys(toCopy.numKeys)
}

// isRoot returns true if the current node is the root node.
func (node *InternalNode) isRoot() bool {
	return node.page.GetPageNum() == ROOT_PN
}

// keyPos returns the offset in the page to the internal node's ith key.
func keyPos(index int64) int64 {
	return KEYS_OFFSET + index*KEY_SIZE
}

// pnPos returns the page offset to the internal node's ith child's pagenumber
func pnPos(index int64) int64 {
	return PNS_OFFSET + index*PN_SIZE
}

// getKeyAt returns the key stored at the given index of the internal node.
// Concurrency note: this InternalNode's page should at least be read-locked before calling.
func (node *InternalNode) getKeyAt(index int64) int64 {
	startPos := keyPos(index)
	key, _ := binary.Varint(node.page.GetData()[startPos : startPos+KEY_SIZE])
	return key
}

// updateKeyAt updates the key at the given index of the internal node.
func (node *InternalNode) updateKeyAt(index int64, newKey int64) {
	// Serialize the key data
	data := make([]byte, KEY_SIZE)
	binary.PutVarint(data, newKey)
	startPos := keyPos(index)
	node.page.Update(data, startPos, KEY_SIZE)
}

// getPNAt returns the pagenumber stored at the given index of the internal node.
// Concurrency note: this InternalNode's page should at least be read-locked before calling.
func (node *InternalNode) getPNAt(index int64) int64 {
	startPos := pnPos(index)
	pagenum, _ := binary.Varint(node.page.GetData()[startPos : startPos+PN_SIZE])
	return pagenum
}

// updatePNAt updates the pagenumber at the given index of the internal node.
func (node *InternalNode) updatePNAt(index int64, newPagenum int64) {
	// Serialize the pagenum data
	data := make([]byte, PN_SIZE)
	binary.PutVarint(data, newPagenum)
	startPos := pnPos(index)
	node.page.Update(data, startPos, PN_SIZE)
}

// getChildAt returns the internal node's ith child.
// Child nodes retrieved via this function must call `PutPage()` accordingly after use.
// Concurrency note: this InternalNode's page should at least be read-locked before calling.
func (node *InternalNode) getChildAt(index int64) (Node, error) {
	// Get the child's page
	pagenum := node.getPNAt(index)
	page, err := node.page.GetPager().GetPage(pagenum)
	if err != nil {
		return &InternalNode{}, err
	}
	return pageToNode(page), nil
}

// getAndLockChildAt write locks and returns the internal node's ith child.
// Child nodes retrieved via this function must call `PutPage()` accordingly after use.
// Concurrency note: this InternalNode's page should at least be read-locked before calling.
func (node *InternalNode) getAndLockChildAt(index int64) (Node, error) {
	// Get the child's page
	pagenum := node.getPNAt(index)
	page, err := node.page.GetPager().GetPage(pagenum)
	if err != nil {
		return &InternalNode{}, err
	}
	page.WLock()
	return pageToNode(page), nil
}

// updateNumKeys updates the numKeys field in the node struct and the underlying page.
func (node *InternalNode) updateNumKeys(newNumKeys int64) {
	node.numKeys = newNumKeys
	// Write the new data to the page
	nKeysData := make([]byte, NUM_KEYS_SIZE)
	binary.PutVarint(nKeysData, newNumKeys)
	node.page.Update(nKeysData, NUM_KEYS_OFFSET, NUM_KEYS_SIZE)
}

/////////////////////////////////////////////////////////////////////////////
////////////////////////// Lock Helper Functions ////////////////////////////
/////////////////////////////////////////////////////////////////////////////

// [CONCURRENCY] Sets the parent pointer of the passed-in child node to this internal node.
func (node *InternalNode) initChild(child Node) {
	// Set the NodeLockHeader parent field to be this node and lock the node.
	switch castedChild := child.(type) {
	case *InternalNode:
		castedChild.parent = node
	case *LeafNode:
		castedChild.parent = node
	}
}

// canSplit returns whether this node has the capability to split in the next insert operation.
func (node *InternalNode) canSplit() bool {
	return node.numKeys == KEYS_PER_INTERNAL_NODE-1
}

// unlockParents unlocks all of this node's locked parents.
func (node *InternalNode) unlockParents() {
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

// unlock unlocks this internal node.
func (node *InternalNode) unlock() {
	node.parent = nil
	node.page.WUnlock()
}
