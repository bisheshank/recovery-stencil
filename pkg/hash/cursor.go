package hash

import (
	"errors"

	"dinodb/pkg/cursor"
	"dinodb/pkg/entry"
)

// HashCursor points to a spot in the hash table.
type HashCursor struct {
	table     *HashIndex
	cellnum   int64
	curBucket *HashBucket
}

// CursorAtStart returns a cursor to the first entry in the hash table.
func (table *HashIndex) CursorAtStart() (cursor.Cursor, error) {
	cursor := HashCursor{table: table, cellnum: 0}

	curPage, err := table.pager.GetPage(ROOT_PN)
	if err != nil {
		return nil, err
	}
	defer table.pager.PutPage(curPage)
	cursor.curBucket = pageToBucket(curPage)
	//if we are in an empty bucket, move to the leftmost non-empty bucket
	if cursor.curBucket.numKeys == 0 {
		noEntries := cursor.Next()
		//if noEntries is true, then all our buckets are empty
		if noEntries {
			return nil, errors.New("all buckets are empty")
		}
	}

	return &cursor, nil
}

// Next moves the cursor ahead by one entry.
// Returns true if we reach the end of our index
func (cursor *HashCursor) Next() bool {
	// If the cursor is at the end of the bucket, try visiting the next bucket.
	if cursor.cellnum+1 >= cursor.curBucket.numKeys {
		// Get the next page number.
		nextPN := cursor.curBucket.page.GetPageNum() + 1
		if nextPN >= cursor.curBucket.page.GetPager().GetNumPages() {
			return true
		}
		// Convert the page to a bucket.
		nextPage, err := cursor.table.pager.GetPage(nextPN)
		if err != nil {
			return true
		}
		defer cursor.table.pager.PutPage(nextPage)
		nextBucket := pageToBucket(nextPage)
		// Reinitialize the cursor.
		cursor.cellnum = 0
		cursor.curBucket = nextBucket
		// If the new bucket is also empty, call next again
		if nextBucket.numKeys == 0 {
			return cursor.Next()
		}
		return false
	}
	// If the cursor is not at the end of the bucket, just move the cursor forward.
	cursor.cellnum++
	return false
}

// GetEntry returns the entry currently pointed to by the cursor.
func (cursor *HashCursor) GetEntry() (entry.Entry, error) {
	if cursor.cellnum > cursor.curBucket.numKeys {
		return entry.Entry{}, errors.New("getEntry: cursor is not pointing at a valid entry")
	}
	if cursor.curBucket.numKeys == 0 {
		return entry.Entry{}, errors.New("getEntry: cursor is in an empty bucket :(")
	}
	entry := cursor.curBucket.getEntry(cursor.cellnum)
	return entry, nil
}

// Close is called when we no longer need to use the cursor anymore.
func (cursor *HashCursor) Close() {
	// Don't actually need to do anything for Hash because the locking
	// is done on a course granularity
}
