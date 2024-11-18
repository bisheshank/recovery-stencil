package btree

import (
	"errors"

	"dinodb/pkg/cursor"
	"dinodb/pkg/entry"
)

// BTreeCursor is a data structure that allows for easy iteration through
// the entries in a B+Tree's leaf nodes in order.
type BTreeCursor struct {
	index    *BTreeIndex // The B+Tree index that this cursor iterates through.
	curNode  *LeafNode   // Current leaf node we are pointing at
	curIndex int64       // The current index within curNode that we are pointing at.
}

// CursorAtStart returns a cursor pointing to the first entry of the B+Tree.
// Cursor's node should be locked upon leaving, and the page should not have been put
func (index *BTreeIndex) CursorAtStart() (cursor.Cursor, error) {
	// Get the root page.
	curPage, err := index.pager.GetPage(index.rootPN)
	if err != nil {
		return nil, err
	}
	// [CONCURRENCY] Locks the root (RLock)
	curPage.RLock()
	curHeader := pageToNodeHeader(curPage)
	// Traverse down the leftmost children until we reach a leaf node.
	for curHeader.nodeType != LEAF_NODE {
		// * holding lock on curPage upon entry
		curNode := pageToInternalNode(curPage)
		leftmostPN := curNode.getPNAt(0)
		curPage, err = index.pager.GetPage(leftmostPN)
		if err != nil {
			// unlock self
			curNode.parent = nil
			index.pager.PutPage(curNode.page)
			curNode.page.RUnlock()
			return nil, err
		}
		// [CONCURRENCY] lock child and then unlock parent
		curPage.RLock()
		curNode.parent = nil
		index.pager.PutPage(curNode.page)
		curNode.page.RUnlock()

		curHeader = pageToNodeHeader(curPage)
	}
	// Set the cursor to point to the first entry in the leftmost leaf node.
	leftmostNode := pageToLeafNode(curPage)
	cursor := &BTreeCursor{index: index, curIndex: 0, curNode: leftmostNode}
	// Account for the edge case where the leftmostNode is empty
	// By adding a call to Next() here if the first node is empty,
	// we can guarantee that the cursor won't be stuck in an
	// empty node
	if cursor.curNode.numKeys == 0 {
		noEntries := cursor.Next()
		//if noEntries is true, then all our leaf nodes are empty
		if noEntries {
			return nil, errors.New("all leaf nodes are empty")
		}
	}
	return cursor, nil
}

// CursorAt returns a cursor pointing to the given key.
// If the key is not found, calls Next() to reach the next entry
// after the position of where key would be.
//
// Hint: use keyToNodeEntry
// Cursor's node should leave locked, and its page should not have been put
func (index *BTreeIndex) CursorAt(key int64) (cursor.Cursor, error) {
	// Get the root page.
	rootPage, err := index.pager.GetPage(index.rootPN)
	if err != nil {
		return nil, err
	}
	// [CONCURRENCY]
	rootPage.RLock()
	rootNode := pageToNode(rootPage)
	// Traverse down the B+Tree to find where the entry with the given key is found
	curNode := rootNode
	for {
		iNode, ok := curNode.(*InternalNode)
		if !ok {
			// curNode must be a leaf node so we have reached the bottom of the tree
			break
		}
		i := iNode.search(key)
		child, err := iNode.getChildAt(i)
		if err != nil {
			return nil, err
		}

		// [CONCURRENCY] lock-crabbing: get child lock, then release parent lock and put its page
		child.getPage().RLock()
		curPage := curNode.getPage()
		index.pager.PutPage(curPage)
		curPage.RUnlock()

		curNode = child
	}

	// Initialize cursor
	cursor := &BTreeCursor{index: index, curIndex: curNode.search(key), curNode: curNode.(*LeafNode)}
	// If the cursor is not pointing at an entry, call Next()
	// This can happen if the entry associated 'key' was previously deleted
	// we can do this because CursorAt() is used only for SelectRange()
	if cursor.curIndex >= cursor.curNode.numKeys {
		cursor.Next()
	}
	return cursor, nil
}

// Next() moves the cursor ahead by one entry. Returns true at the end of the BTree.
// Cursor's node should enter and leave locked.
// The node the cursor is in upon return's page should not have been put
func (cursor *BTreeCursor) Next() (atEnd bool) {
	// [CONCURRENCY]
	// If the cursor is at the end of the node, go to the next node.
	if cursor.curIndex+1 >= cursor.curNode.numKeys {
		// Get the next node's page number.
		nextPN := cursor.curNode.rightSiblingPN
		if nextPN < 0 {
			return true
		}
		// Convert the page into a node.
		nextPage, err := cursor.index.pager.GetPage(nextPN)
		if err != nil {
			return true
		}
		// [CONCURRENCY]
		nextPage.RLock()
		cursor.index.pager.PutPage(cursor.curNode.page)
		cursor.curNode.page.RUnlock()

		nextNode := pageToLeafNode(nextPage)
		// Reinitialize the cursor.
		cursor.curIndex = 0
		cursor.curNode = nextNode
		// If the next node is empty, step to the next node.
		// If no deletes are called, then this should never happen
		if nextNode.numKeys == 0 {
			return cursor.Next()
		}
		return false
	}
	// Else, just move the cursor forward.
	cursor.curIndex++
	return false
}

// GetEntry returns the entry currently pointed to by the cursor.
func (cursor *BTreeCursor) GetEntry() (entry.Entry, error) {
	// Check if we're retrieving a non-existent entry.
	if cursor.curIndex > cursor.curNode.numKeys {
		return entry.Entry{}, errors.New("getEntry: cursor is not pointing at a valid entry")
	}
	if cursor.curNode.numKeys == 0 {
		return entry.Entry{}, errors.New("getEntry: cursor is in an empty node :(")
	}
	entry := cursor.curNode.getEntry(cursor.curIndex)
	return entry, nil
}

// Close is called to unlock the page of the node the Cursor is in
// once the Cursor is no longer being used.
func (cursor *BTreeCursor) Close() {
	// Unlock the Cursor's node node once we are done with the cursor
	// and put the page of the node the cursor was in
	cursor.index.pager.PutPage(cursor.curNode.page)
	cursor.curNode.page.RUnlock()
}
