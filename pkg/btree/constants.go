package btree

import (
	"dinodb/pkg/pager"
	"encoding/binary"
)

// We'll always maintain the invariant that the root's pagenum is 0.
// This saves us the effort of having to find the root node every time
// we open the database.
var ROOT_PN int64 = 0

// Entry constants.
const ENTRYSIZE int64 = binary.MaxVarintLen64 * 2

// Node header constants.
const (
	NODETYPE_OFFSET  int64 = 0
	NODETYPE_SIZE    int64 = 1
	NUM_KEYS_OFFSET  int64 = NODETYPE_OFFSET + NODETYPE_SIZE
	NUM_KEYS_SIZE    int64 = binary.MaxVarintLen64
	NODE_HEADER_SIZE int64 = NODETYPE_SIZE + NUM_KEYS_SIZE
)

// Leaf node header constants.
const (
	RIGHT_SIBLING_PN_OFFSET int64 = NODE_HEADER_SIZE
	RIGHT_SIBLING_PN_SIZE   int64 = binary.MaxVarintLen64
	LEAF_NODE_HEADER_SIZE   int64 = NODE_HEADER_SIZE + RIGHT_SIBLING_PN_SIZE
	ENTRIES_PER_LEAF_NODE   int64 = ((pager.Pagesize - LEAF_NODE_HEADER_SIZE) / ENTRYSIZE) - 1
)

// Internal node header constants.
const (
	KEY_SIZE                  int64 = binary.MaxVarintLen64
	PN_SIZE                   int64 = binary.MaxVarintLen64
	INTERNAL_NODE_HEADER_SIZE int64 = NODE_HEADER_SIZE
	ptrSpace                  int64 = pager.Pagesize - INTERNAL_NODE_HEADER_SIZE - KEY_SIZE
	KEYS_PER_INTERNAL_NODE    int64 = (ptrSpace / (KEY_SIZE + PN_SIZE)) - 1
	KEYS_OFFSET               int64 = INTERNAL_NODE_HEADER_SIZE
	KEYS_SIZE                 int64 = KEY_SIZE * (KEYS_PER_INTERNAL_NODE + 1)
	PNS_OFFSET                int64 = KEYS_OFFSET + KEYS_SIZE
)

// [CONCURRENCY]
var SUPER_NODE = &InternalNode{NodeHeader: NodeHeader{INTERNAL_NODE, 0, &pager.Page{}}}
