package pager_test

import (
	"bytes"
	"testing"

	"dinodb/pkg/config"
	"dinodb/pkg/pager"
	"dinodb/test/utils"
)

// setupPager creates a new pager and checks for creation errors.
// Returns the new pager and the file name of the backing .db file
func setupPager(t *testing.T) *pager.Pager {
	t.Parallel()
	dbname := utils.GetTempDbFile(t)
	p, err := pager.New(dbname)
	if err != nil {
		t.Fatal("Failed to create a new pager:", err)
	}

	utils.EnsureCleanup(t, func() {
		// Don't check close error since we are only concerned with resource cleanup
		_ = p.Close()
	})
	return p
}

// getNewPage wraps a call to Pager.GetNewPage() with error checking.
// If deferPut is true, queues the page to be put when the test ends.
func getNewPage(t *testing.T, p *pager.Pager, deferPut bool) *pager.Page {
	page, err := p.GetNewPage()
	if err != nil {
		t.Fatal("Error getting new page:", err)
	}

	if deferPut {
		utils.EnsureCleanup(t, func() {
			// Don't need to check put error since we explicitly check in testTooManyPuts
			_ = p.PutPage(page)
		})
	}
	return page
}

// getPage wraps a call to Pager.GetPage(pagenum) with error checking.
// If deferPut is true, queues the page to be put when the test ends.
func getPage(t *testing.T, p *pager.Pager, pagenum int64, deferPut bool) *pager.Page {
	page, err := p.GetPage(pagenum)
	if err != nil {
		t.Fatalf("Error getting existing page %d: %s", pagenum, err)
	}

	if deferPut {
		utils.EnsureCleanup(t, func() {
			err = p.PutPage(page)
			if err != nil {
				t.Errorf("Error putting page %d: %s", page.GetPageNum(), err)
			}
		})
	}
	return page
}

// closeAndReopen closes a pager then reopens it with the same database file,
// failing the test if any errors are returned
func closeAndReopen(t *testing.T, p *pager.Pager) {
	err := p.Close()
	if err != nil {
		t.Fatal("Failed to close pager:", err)
	}

	err = p.Open(p.GetFileName())
	if err != nil {
		t.Fatal("Failed to open pager:", err)
	}
}

func TestPager(t *testing.T) {
	t.Run("NewPager", testNewPager)
	t.Run("GetNewPage", testGetNewPage)
	t.Run("GetPagePagenumber", testGetPagePagenumber)
	t.Run("NegativePagenumber", testNegativePagenumber)
	t.Run("MaxGetNewPages", testMaxGetNewPages)
	t.Run("FlushOnePage", testFlushOnePage)
	t.Run("TooManyPuts", testTooManyPuts)
	t.Run("PincountsOnClose", testPincountsOnClose)
	t.Run("GetExistingChangedPage", testGetExistingChangedPage)
	t.Run("GetNewPagesStress", testGetNewPagesStress)
}

/*
Sets up a new pager and then closes it, checking that no errors
happen along the way.
*/
func testNewPager(t *testing.T) {
	_ = setupPager(t)
}

/*
Checks that the first call to GetNewPage returns a dirty page with
the right pager and page number of 0.
*/
func testGetNewPage(t *testing.T) {
	p := setupPager(t)
	page := getNewPage(t, p, true)
	if page.GetPager() != p {
		t.Error("New page has bad pager field")
	}
	if page.GetPageNum() != 0 {
		t.Error("Expected new page to have pagenum 0, but found pagenum", page.GetPageNum())
	}
	if !page.IsDirty() {
		t.Error("Expected new page to be dirty, but it wasn't")
	}
}

/*
Calls GetNewPage twice and tries to retrieve the pagenum 1,
checking that the pages returned have the correct pagenum.
*/
func testGetPagePagenumber(t *testing.T) {
	p := setupPager(t)
	// Get pages
	p1 := getNewPage(t, p, true)
	p2 := getNewPage(t, p, true)
	p3 := getPage(t, p, 1, true)
	// check for expected page returned from the GetPage()s
	if p1.GetPageNum() != 0 {
		t.Errorf("Expected pagenum %d for new page, but found %d", 0, p1.GetPageNum())
	}
	if p2.GetPageNum() != 1 {
		t.Errorf("Expected pagenum %d for new page, but found %d", 1, p2.GetPageNum())
	}
	if p3.GetPageNum() != 1 {
		t.Errorf("Expected pagenum %d for existing page, but found %d", 1, p3.GetPageNum())
	}
}

/*
Checks that GetPage with a negative pagenum returns an error
*/
func testNegativePagenumber(t *testing.T) {
	p := setupPager(t)
	_, err := p.GetPage(-1)
	if err == nil {
		t.Fatal("Expected GetPage to return an error upon negative pagenumber request")
	}
}

/*
Checks well-formedness of GetNewPage in relation to buffer cache size.
Fills up the active pages in the cache, and then checks that getting
more unique pages when the cache is filled does not work.

Uses GetNewPage to get all the possible number of pages up to config.MaxPagesInBuffer
and checks that it works. Then, try to GetNewPage again and check that it
fails and returns an error.
*/
func testMaxGetNewPages(t *testing.T) {
	p := setupPager(t)
	for i := 0; i < config.MaxPagesInBuffer; i++ {
		_ = getNewPage(t, p, true)
	}
	page, err := p.GetNewPage()
	if err == nil {
		_ = p.PutPage(page)
		t.Fatal("Should have returned an error for running out of pages")
	}
}

/*
Gets a new page, writes to it, flushes it, and closes the pager.
Upon reopening the pager and getting the same page, the data should
be consistently updated in the page.
*/
func testFlushOnePage(t *testing.T) {
	p := setupPager(t)
	// Write some data to page 0
	page := getNewPage(t, p, false)
	data := []byte("hello")
	page.Update(data, 0, int64(len(data)))
	_ = p.PutPage(page)

	p.FlushPage(page)
	closeAndReopen(t, p)

	page = getPage(t, p, 0, true)
	// the data should be the same
	if !bytes.Equal(page.GetData()[:len(data)], data) {
		t.Fatal("Data not flushed properly")
	}
}

/*
Tests that PutPage() works as expected by getting a page and putting
it away and checking that it works properly + did not error.
Then, call PutPage() again on the page and check that an error is returned
because now the pincount would be < 0.
*/
func testTooManyPuts(t *testing.T) {
	p := setupPager(t)
	page := getNewPage(t, p, false)
	// Good put should not error
	err := p.PutPage(page)
	if err != nil {
		t.Fatal("Initial put page shouldn't fail, but failed with:", err)
	}
	// Bad put that brings pincount < 0 should return error
	err = p.PutPage(page)
	if err == nil {
		t.Fatal("PutPage should fail because pincount < 0, but it didn't")
	}
}

/*
Tests that upon closing a pager with pages still pinned, an error
is returned from Close.
*/
func testPincountsOnClose(t *testing.T) {
	p := setupPager(t)
	_ = getNewPage(t, p, false)
	// Try closing without unpinning pages
	err := p.Close()
	if err == nil {
		t.Fatal("Did not receive expected error about pages still being pinned on close")
	}
}

/*
Writes data to a newly created page without flushing.
Then makes sure that GetPage returns the same page with the new data
(testing that the page is retrieved from the buffer and not disk).
*/
func testGetExistingChangedPage(t *testing.T) {
	p := setupPager(t)
	//get a page and write to it, but don't flush it
	p1 := getNewPage(t, p, true)
	data := []byte("test data")
	p1.Update(data, 0, int64(len(data)))
	//get the same page and check that the data is in it
	p2 := getPage(t, p, 0, true)
	// the data should be the same
	if p1 != p2 {
		t.Error("Pages returned are not the same")
	}
	if !bytes.Equal(p2.GetData()[:len(data)], data) {
		t.Error("Data not retained in buffer cache")
	}
}

/*
Calls GetNewPage 10,000 times and ensures each page has consecutively
increasing page numbers.
*/
func testGetNewPagesStress(t *testing.T) {
	p := setupPager(t)
	// Get 10,0000 new pages.
	for i := 0; i < 10000; i++ {
		page := getNewPage(t, p, false)
		if page.GetPageNum() != int64(i) {
			t.Fatalf("Expected new page to have pagenum %d, but was %d", i, page.GetPageNum())
		}
		_ = p.PutPage(page)
	}
}