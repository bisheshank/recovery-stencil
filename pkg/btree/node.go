package btree

import (
	"encoding/binary"
	"fmt"
	"io"

	"dinodb/pkg/pager"
)

/////////////////////////////////////////////////////////////////////////////
///////////////////////// Structs and interfaces ////////////////////////////
/////////////////////////////////////////////////////////////////////////////

// Split is a supporting data structure to propagate information
// needed to implement splits up our B+tree after inserts.
type Split struct {
	isSplit bool  // A flag that's set to true if a split occurs.
	key     int64 // The median key that is being pushed up.
	leftPN  int64 // The pagenumber for the left node.
	rightPN int64 // The pagenumber for the right node.
}

// Node defines a common interface for leaf and internal nodes.
type Node interface {
	// insert traverses down the B+Tree and inserts the specified
	// key-value pair into a leaf node. Cannot insert a duplicate key.
	// Returns a Split with relevant data to be used by the caller
	// if the insertion results in the node splitting.
	//
	// If the update flag is true, then insert will perform an update instead,
	// returning an error if an existing entry to overwrite is not found.
	insert(key int64, value int64, update bool) (Split, error)

	// delete traverses down the B+Tree and removes the entry with the given key
	// from the leaf nodes if it exists.
	// Note that delete does not implement merging of node (see handout for more details).
	delete(key int64)

	// get tries to find the value associated with the given key in the B+Tree,
	// traversing down to the leaf nodes. It returns a boolean indicating whether
	// the key was found in the node and the associated value if found.
	get(key int64) (value int64, found bool)

	// Helper methods added for convenience
	search(searchKey int64) int64
	// printNode writes a string representation of the node to the specified
	printNode(io.Writer, string, string)
	// getPage returns the node's underlying page where it's data is stored.
	getPage() *pager.Page
	getNodeType() NodeType
}

// NodeType identifies if a node is a leaf node or an internal node.
type NodeType bool

const (
	INTERNAL_NODE NodeType = false
	LEAF_NODE     NodeType = true
)

// NodeHeaders contain metadata common to all types of nodes
type NodeHeader struct {
	nodeType NodeType    // The type of the node (either leaf or internal).
	numKeys  int64       // The number of keys currently stored in the node.
	page     *pager.Page // The page that holds the node's data
}

/////////////////////////////////////////////////////////////////////////////
//////////////////////// Generic Helper Functions ///////////////////////////
/////////////////////////////////////////////////////////////////////////////

// initPage resets the page's data then sets the nodeType bit.
func initPage(page *pager.Page, nodeType NodeType) {
	newData := make([]byte, pager.Pagesize)
	// Set the nodeType bit for leaf nodes (don't need to set InternalNode bit since it is 0)
	if nodeType == LEAF_NODE {
		newData[NODETYPE_OFFSET] = 1
	}
	page.Update(newData, 0, pager.Pagesize)
}

// pageToNode returns the node corresponding to the given page.
// Concurrency note: the given page must at least be read-locked before calling.
func pageToNode(page *pager.Page) Node {
	nodeHeader := pageToNodeHeader(page)
	if nodeHeader.nodeType == LEAF_NODE {
		return pageToLeafNode(page)
	}
	return pageToInternalNode(page)
}

// pageToNodeHeader returns node header data from the given page.
// Concurrency note: the given page must at least be read-locked before calling.
func pageToNodeHeader(page *pager.Page) NodeHeader {
	var nodeType NodeType
	if page.GetData()[NODETYPE_OFFSET] == 0 {
		nodeType = INTERNAL_NODE
	} else {
		nodeType = LEAF_NODE
	}
	numKeys, _ := binary.Varint(
		page.GetData()[NUM_KEYS_OFFSET : NUM_KEYS_OFFSET+NUM_KEYS_SIZE],
	)
	return NodeHeader{
		nodeType: nodeType,
		numKeys:  numKeys,
		page:     page,
	}
}

// [CONCURRENCY] Sets the root node's parent pointer to the SUPER_NODE.
func initRootNode(root Node) {
	switch castedRootNode := root.(type) {
	case *InternalNode:
		castedRootNode.parent = SUPER_NODE
	case *LeafNode:
		castedRootNode.parent = SUPER_NODE
	}
}

// [CONCURRENCY]
// lockRoot locks the super node and the specified root node's page.
func lockRoot(rootPage *pager.Page) {
	SUPER_NODE.page.WLock()
	rootPage.WLock()
}

// [CONCURRENCY]
// Force unlocks the super node and the root node.
// Is backup function that should only be called
// if the student has not finished concurrency yet.
func unsafeUnlockRoot(root Node) {
	// Lock the root node.
	switch castedRootNode := root.(type) {
	case *InternalNode:
		if castedRootNode.parent != nil {
			// Emit a warning to disable this function call.
			fmt.Println("WARNING: unsafeUnlockRoot was called. This function will only be called if theroot node is not being unlocked properly.")
			castedRootNode.parent = nil
			castedRootNode.page.WUnlock()
			SUPER_NODE.page.WUnlock()
		}
	case *LeafNode:
		if castedRootNode.parent != nil {
			// Emit a warning to disable this function call.
			fmt.Println("WARNING: unsafeUnlockRoot was called. This function will only be called if the root node is not being unlocked properly.")
			castedRootNode.parent = nil
			castedRootNode.page.WUnlock()
			SUPER_NODE.page.WUnlock()
		}
	}
}
