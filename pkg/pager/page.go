package pager

import (
	"sync"
	"sync/atomic"
)

// NoPage is the pagenum for when there is no page being held
const NoPage = -1

// Page caches a page from disk and stores additional metadata.
type Page struct {
	pager      *Pager       // Pointer to the pager that this page belongs to
	pagenum    int64        // Unique identifier for the page also denoting it's position stored in the pager's file
	pinCount   atomic.Int64 // The number of active references to this page
	dirty      bool         // Flag on whether the page's data has changed and needs to be written to disk
	rwlock     sync.RWMutex // Reader-writer lock on the page struct itself
	data       []byte       // Serialized data (the actual 4096 bytes of the page)
	updateLock sync.Mutex   // Mutex for updating the page's data
}

// GetPager returns the pager this page belongs to.
func (page *Page) GetPager() *Pager {
	return page.pager
}

// GetPageNum returns the page's pagenum (unique identifier).
func (page *Page) GetPageNum() int64 {
	return page.pagenum
}

// IsDirty reports whether the page's data has changed and needs to be written to disk.
func (page *Page) IsDirty() bool {
	return page.dirty
}

// SetDirty changes the dirty status of a page.
func (page *Page) SetDirty(dirty bool) {
	page.dirty = dirty
}

// GetData returns the byte data held by the page.
func (page *Page) GetData() []byte {
	return page.data
}

// Get increments the pin count, indicating that another process is using this page.
func (page *Page) Get() {
	page.pinCount.Add(1)
}

// Put decrements the pincount, indicating that a process is done using this page.
func (page *Page) Put() int64 {
	return page.pinCount.Add(-1)
}

// Update updates this page with `size` bytes of the the given data slice at the specified offset.
func (page *Page) Update(data []byte, offset int64, size int64) {
	page.updateLock.Lock()
	defer page.updateLock.Unlock()
	page.dirty = true
	copy(page.data[offset:offset+size], data)
}

// [CONCURRENCY] Grab a writers lock on the page.
func (page *Page) WLock() {
	page.rwlock.Lock()
}

// [CONCURRENCY] Release a writers lock.
func (page *Page) WUnlock() {
	page.rwlock.Unlock()
}

// [CONCURRENCY] Grab a readers lock on the page.
func (page *Page) RLock() {
	page.rwlock.RLock()
}

// [CONCURRENCY] Release a readers lock.
func (page *Page) RUnlock() {
	page.rwlock.RUnlock()
}

// [RECOVERY] Grab the update lock.
func (page *Page) LockUpdates() {
	page.updateLock.Lock()
}

// [RECOVERY] Release the update lock.
func (page *Page) UnlockUpdates() {
	page.updateLock.Unlock()
}
