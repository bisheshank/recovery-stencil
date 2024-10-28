package pager

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"dinodb/pkg/list"
	"dinodb/pkg/repl"
)

// Creates a Pager REPL for testing the Pager with.
func PagerRepl() (*repl.REPL, error) {
	// Initialize pager.
	p, err := New("data/pager.tmp")
	if err != nil {
		return nil, err
	}
	// Initialize repl.
	r := repl.NewRepl()

	r.AddCommand("pager_print", func(payload string, replConfig *repl.REPLConfig) (string, error) {
		return HandlePagerPrint(p, payload)
	}, "Print out the state of the pager. usage: pager_print")

	r.AddCommand("pager_get", func(payload string, replConfig *repl.REPLConfig) (string, error) {
		return "", HandlePagerGet(p, payload)
	}, "Get a page into the pager. usage: pager_get <page_num>")

	r.AddCommand("pager_new", func(payload string, replConfig *repl.REPLConfig) (string, error) {
		return "", HandlePagerNew(p, payload)
	}, "Allocate a new page. usage: pager_new")

	r.AddCommand("pager_write", func(payload string, replConfig *repl.REPLConfig) (string, error) {
		return "", HandlePagerWrite(p, payload)
	}, "Write data to a page. usage: pager_write <page_num> <payload>")

	r.AddCommand("pager_read", func(payload string, replConfig *repl.REPLConfig) (string, error) {
		return HandlePagerRead(p, payload)
	}, "Read data from a page. usage: pager_read <page_num>")

	r.AddCommand("pager_pin", func(payload string, replConfig *repl.REPLConfig) (string, error) {
		return "", HandlePagerPin(p, payload)
	}, "Pin a page. usage: pager_pin <page_num>")

	r.AddCommand("pager_unpin", func(payload string, replConfig *repl.REPLConfig) (string, error) {
		return "", HandlePagerUnpin(p, payload)
	}, "Unpin a page. usage: pager_unpin <page_num>")

	r.AddCommand("pager_flush", func(payload string, replConfig *repl.REPLConfig) (string, error) {
		return "", HandlePagerFlush(p, payload)
	}, "Flush a page. usage: pager_flush <page_num>")

	r.AddCommand("pager_flushall", func(payload string, replConfig *repl.REPLConfig) (string, error) {
		return "", HandlePagerFlushAll(p, payload)
	}, "Flush all pages. usage: pager_flushall")

	return r, nil
}

// Function to print out state of the pager.
func HandlePagerPrint(p *Pager, payload string) (output string, err error) {
	fields := strings.Fields(payload)
	numFields := len(fields)
	// Usage: pager_print
	if numFields != 1 {
		return "", errors.New("usage: pager_print")
	}

	w := new(strings.Builder)
	// Print numPages, freeList, unpinnedList, pinnedList, pageTable.
	io.WriteString(w, fmt.Sprintf("numPages: %v\n", p.numPages))
	io.WriteString(w, "freeList: ")
	p.freeList.Map(func(l *list.Link) {
		io.WriteString(w, fmt.Sprintf("(pagenum: %v), ", l.GetValue().(*Page).GetPageNum()))
	})
	io.WriteString(w, "\nunpinnedList: ")
	p.unpinnedList.Map(func(l *list.Link) {
		page := l.GetValue().(*Page)
		io.WriteString(w, fmt.Sprintf("(pagenum: %v, pincount: %v), ", page.GetPageNum(), page.pinCount.Load()))
	})
	io.WriteString(w, "\npinnedList: ")
	p.pinnedList.Map(func(l *list.Link) {
		page := l.GetValue().(*Page)
		io.WriteString(w, fmt.Sprintf("(pagenum: %v, pincount: %v), ", page.GetPageNum(), page.pinCount.Load()))
	})
	io.WriteString(w, "\npageTable: ")
	for pNum := range p.pageTable {
		io.WriteString(w, fmt.Sprintf("%v, ", pNum))
	}
	io.WriteString(w, "\n")
	return w.String(), nil
}

// Function to get an existing page and pull; errors if requesting a page that has not been allocated.
func HandlePagerGet(p *Pager, payload string) (err error) {
	fields := strings.Fields(payload)
	numFields := len(fields)
	// Usage: pager_get <page_num>
	if numFields != 2 {
		return fmt.Errorf("usage: pager_get <page_num>")
	}
	// Get page num.
	var pNum int
	if pNum, err = strconv.Atoi(fields[1]); err != nil {
		return err
	}
	// Check if allocated.
	if int64(pNum) >= p.numPages {
		return errors.New("error: haven't allocated that page number yet")
	}
	p.GetPage(int64(pNum))
	return nil
}

// Function to allocate a new page.
func HandlePagerNew(p *Pager, payload string) (err error) {
	fields := strings.Fields(payload)
	numFields := len(fields)
	// Usage: pager_new
	if numFields != 1 {
		return fmt.Errorf("usage: pager_new")
	}
	p.GetNewPage()
	return nil
}

// Function to write data to a page.
func HandlePagerWrite(p *Pager, payload string) (err error) {
	fields := strings.Fields(payload)
	numFields := len(fields)
	// Usage: pager_write <page_num> <payload>
	if numFields != 3 {
		return fmt.Errorf("usage: pager_write <page_num> <payload>")
	}
	// Get page num.
	var pNum int
	if pNum, err = strconv.Atoi(fields[1]); err != nil {
		return err
	}
	// Check that this page is in our pageTable
	link, found := p.pageTable[int64(pNum)]
	if !found {
		return errors.New("page not found; did you pager_get it first?")
	}
	// Cast and write.
	page := link.GetValue().(*Page)
	page.Get()
	data := []byte(fields[2])
	page.Update(data, 0, int64(len(data)))
	p.PutPage(page)
	return nil
}

// Function to print out the contents of a page.
func HandlePagerRead(p *Pager, payload string) (output string, err error) {
	fields := strings.Fields(payload)
	numFields := len(fields)
	// Usage: pager_read <page_num>
	if numFields != 2 {
		return "", fmt.Errorf("usage: pager_read <page_num>")
	}
	// Get page num.
	var pNum int
	if pNum, err = strconv.Atoi(fields[1]); err != nil {
		return "", err
	}
	// Check that this page is in our pageTable
	link, found := p.pageTable[int64(pNum)]
	if !found {
		return "", errors.New("page not found; did you pager_get it first?")
	}
	// Print.
	page := link.GetValue().(*Page)
	page.Get()
	w := new(strings.Builder)
	io.WriteString(w, string(page.GetData()))
	io.WriteString(w, "\n")
	p.PutPage(page)
	return w.String(), nil
}

// Function to pin a page.
func HandlePagerPin(p *Pager, payload string) (err error) {
	fields := strings.Fields(payload)
	numFields := len(fields)
	// Usage: pager_pin <page_num>
	if numFields != 2 {
		return fmt.Errorf("usage: pager_pin <page_num>")
	}
	// Get page num.
	var pNum int
	if pNum, err = strconv.Atoi(fields[1]); err != nil {
		return err
	}
	// Check that this page is in our pageTable
	link, found := p.pageTable[int64(pNum)]
	if !found {
		return errors.New("page not found; did you pager_get it first?")
	}
	// Pin.
	if link.GetList() == p.unpinnedList {
		link.PopSelf()
		newLink := p.pinnedList.PushHead(link.GetValue())
		p.pageTable[int64(pNum)] = newLink
	}
	page := link.GetValue().(*Page)
	page.Get()
	return nil
}

// Function to unpin a page.
func HandlePagerUnpin(p *Pager, payload string) (err error) {
	fields := strings.Fields(payload)
	numFields := len(fields)
	// Usage: pager_unpin <page_num>
	if numFields != 2 {
		return fmt.Errorf("usage: pager_unpin <page_num>")
	}
	// Get page num.
	var pNum int
	if pNum, err = strconv.Atoi(fields[1]); err != nil {
		return err
	}
	// Check that this page is in our pageTable
	link, found := p.pageTable[int64(pNum)]
	if !found {
		return errors.New("page not found; did you pager_get it first?")
	}
	// Unpin.
	page := link.GetValue().(*Page)
	p.PutPage(page)
	return nil
}

// Function to flush a page.
func HandlePagerFlush(p *Pager, payload string) (err error) {
	fields := strings.Fields(payload)
	numFields := len(fields)
	// Usage: pager_flush <page_num>
	if numFields != 2 {
		return fmt.Errorf("usage: pager_flush <page_num>")
	}
	// Get page num.
	var pNum int
	if pNum, err = strconv.Atoi(fields[1]); err != nil {
		return err
	}
	// Check that this page is in our pageTable
	link, found := p.pageTable[int64(pNum)]
	if !found {
		return errors.New("page not found; did you pager_get it first?")
	}
	// Flush.
	page := link.GetValue().(*Page)
	p.FlushPage(page)
	return nil
}

// Function to flush all pages.
func HandlePagerFlushAll(p *Pager, payload string) (err error) {
	fields := strings.Fields(payload)
	numFields := len(fields)
	// Usage: pager_flushall
	if numFields != 1 {
		return fmt.Errorf("usage: pager_flushall")
	}
	// Flush all.
	p.FlushAllPages()
	return nil
}
