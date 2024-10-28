package go_test

import (
	"testing"

	"dinodb/pkg/list"
)

func verifyList(t *testing.T, list *list.List, data []interface{}) {
	listdata := make([]interface{}, 0)
	curr := list.PeekHead()
	for curr != nil {
		listdata = append(listdata, curr.GetValue())
		curr = curr.GetNext()
	}
	if len(listdata) != len(data) {
		t.Fatal("lists of unequal size")
	}
	if len(listdata) != len(data) {
		t.Fatal("lists of unequal size")
	}
	for i := 0; i < len(data); i++ {
		if listdata[i] == nil || listdata[i] != data[i] {
			t.Fatalf("lists not equal; got %v, expected %v.", listdata[i], data[i])
		}
	}
}

func TestList(t *testing.T) {
	t.Run("EmptyList", testEmptyList)
	t.Run("SingletonList", testSingletonList)
	t.Run("PushHeadIntList", testPushHeadIntList)
	t.Run("PushTailIntList", testPushTailIntList)
	t.Run("FindExists", testFindExists)
	t.Run("FindNotExists", testFindNotExists)
	t.Run("FindEmptyList", testFindEmptyList)
	t.Run("Map", testMap)
	t.Run("GetList", testGetList)
	t.Run("PopSelf", testPopSelf)
	t.Run("PopNewHead", testPopNewHead)
}

/* Checks that list fields are initialized properly upon creation.*/
func testEmptyList(t *testing.T) {
	list := list.NewList()
	if list.PeekHead() != nil {
		t.Fatal("bad list initialization")
	}
	if list.PeekTail() != nil {
		t.Fatal("bad list initialization")
	}
}

/*
Tests that in a list with only one element,
the head of the list is the same as the tail of the list
*/
func testSingletonList(t *testing.T) {
	list := list.NewList()
	list.PushHead(5)
	if list.PeekHead() != list.PeekTail() {
		t.Fatal("head not equal to tail in singleton list")
	}
}

/*
Adds multiple elements to the head of the list and tests that
the order of elements is correct and that the head and tail values
are correct.
*/
func testPushHeadIntList(t *testing.T) {
	l := list.NewList()
	l.PushHead(1)
	l.PushHead(2)
	l.PushHead(3)
	l.PushHead(4)
	l.PushHead(5)
	if l.PeekHead() == nil || l.PeekHead().GetValue() != 5 {
		t.Fatal("bad peekhead")
	}
	if l.PeekTail() == nil || l.PeekTail().GetValue() != 1 {
		t.Fatal("bad peektail")
	}
	verifyList(t, l, []interface{}{5, 4, 3, 2, 1})
}

/*
Adds multiple elements to the tail of the list and tests that
the order of elements is correct and that the head and tail values
are correct.
*/
func testPushTailIntList(t *testing.T) {
	l := list.NewList()
	l.PushTail(1)
	l.PushTail(2)
	l.PushTail(3)
	l.PushTail(4)
	l.PushTail(5)
	if l.PeekHead() == nil || l.PeekHead().GetValue() != 1 {
		t.Fatal("bad peekhead")
	}
	if l.PeekTail() == nil || l.PeekTail().GetValue() != 5 {
		t.Fatal("bad peektail")
	}
	verifyList(t, l, []interface{}{1, 2, 3, 4, 5})
}

/*
Tests that the Find() method works properly when searching for
values that exist in the list.
*/
func testFindExists(t *testing.T) {
	for i := 1; i <= 5; i++ {
		l := list.NewList()
		l.PushHead(1)
		l.PushHead(2)
		l.PushHead(3)
		l.PushHead(4)
		l.PushHead(5)
		lambda := func(l *list.Link) bool { return l.GetValue() == i }
		val := l.Find(lambda)
		if val == nil || val.GetValue() != i {
			t.Fatal("found incorrect value")
		}
	}
}

/*
Tests that the Find() method works properly when searching for
values that do NOT exist in the list.
*/
func testFindNotExists(t *testing.T) {
	l := list.NewList()
	l.PushHead(1)
	l.PushHead(2)
	l.PushHead(3)
	l.PushHead(4)
	l.PushHead(5)
	lambda := func(l *list.Link) bool { return l.GetValue() == 6 }
	val := l.Find(lambda)
	if val != nil {
		t.Fatal("found non-existent value")
	}
}

/*
Tests that the Find() method works properly when searching for
values in an empty list.
*/
func testFindEmptyList(t *testing.T) {
	l := list.NewList()
	lambda := func(l *list.Link) bool { return l.GetValue() == 0 }
	val := l.Find(lambda)
	if val != nil {
		t.Fatal("found a value in an empty list")
	}
}

/*
Tests that Map() works validly and successfully applies a simple
arithmetic function to all values of links in a list.
*/
func testMap(t *testing.T) {
	l := list.NewList()
	l.PushHead(1)
	l.PushHead(2)
	l.PushHead(3)
	l.PushHead(4)
	l.PushHead(5)
	lambda := func(x *list.Link) { x.SetValue(x.GetValue().(int) + 10) }
	l.Map(lambda)
	verifyList(t, l, []interface{}{15, 14, 13, 12, 11})
}

/*
Tests that the list returned by GetList() on a given link is
the expected list
*/
func testGetList(t *testing.T) {
	list := list.NewList()
	list.PushHead(1)
	val := list.PeekHead()
	if val.GetList() != list {
		t.Fatal("bad getlist")
	}
}

/*
Tests that the PopSelf() method works properly on a generic case.
Calls PopSelf on a link in the middle of the list
*/
func testPopSelf(t *testing.T) {
	l := list.NewList()
	l.PushHead(1)
	l.PushHead(2)
	l.PushHead(3)
	l.PushHead(4)
	l.PushHead(5)
	verifyList(t, l, []interface{}{5, 4, 3, 2, 1})

	lambda := func(l *list.Link) bool { return l.GetValue() == 4 }
	val := l.Find(lambda)
	val.PopSelf()
	verifyList(t, l, []interface{}{5, 3, 2, 1})
}

/*
Tests that the head and tail of the list update
properly when PopSelf() is called on the head of a list.
*/
func testPopNewHead(t *testing.T) {
	l := list.NewList()
	l.PushHead(1)
	l.PushHead(2)
	elt1 := l.Find(func(x *list.Link) bool { return x.GetValue() == 1 })
	elt2 := l.Find(func(x *list.Link) bool { return x.GetValue() == 2 })
	elt2.PopSelf()
	if l.PeekHead() != elt1 {
		t.Fatal("bad pop, head not updated")
	}
	if l.PeekTail() != elt1 {
		t.Fatal("bad pop, tail not updated")
	}
}
