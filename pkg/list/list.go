package list

// List struct.
type List struct {
	head *Link
	tail *Link
}

// Create a new list.
func NewList() *List {
	nlist := List{nil, nil}
	return &nlist
}

// Get a pointer to the head of the list.
func (list *List) PeekHead() *Link {
	return list.head
}

// Get a pointer to the tail of the list.
func (list *List) PeekTail() *Link {
	return list.tail
}

// Add an element to the start of the list. Returns the added link.
func (list *List) PushHead(value interface{}) *Link {
	newlink := &Link{list, nil, list.head, value}
	if list.head != nil {
		list.head.prev = newlink
	}
	list.head = newlink
	if list.tail == nil {
		list.tail = newlink
	}
	return newlink
}

// Add an element to the end of the list. Returns the added link.
func (list *List) PushTail(value interface{}) *Link {
	newlink := &Link{list, list.tail, nil, value}
	if list.tail != nil {
		list.tail.next = newlink
	}
	list.tail = newlink
	if list.head == nil {
		list.head = newlink
	}
	return newlink
}

// Find an element in a list given a boolean function, f, that evaluates to true on the desired element.
func (list *List) Find(f func(*Link) bool) *Link {
	newlist := &List{list.head, list.tail}
	for newlist.head != nil {
		if f(newlist.head) {
			return newlist.head
		}
		newlist.head = newlist.head.next
	}
	return nil
}

// Apply a function to every element in the list.
// Note: Map directly mutates the links in the list
func (list *List) Map(f func(*Link)) {
	newlist := &List{list.head, list.tail}
	for newlist.head != nil {
		f(newlist.head)
		newlist.head = newlist.head.next
	}
	list = newlist
}

// Link struct.
type Link struct {
	list  *List
	prev  *Link
	next  *Link
	value interface{}
}

// Get the list that this link is a part of.
func (link *Link) GetList() *List {
	return link.list
}

// Get the link's value.
func (link *Link) GetValue() interface{} {
	return link.value
}

// Set the link's value.
func (link *Link) SetValue(value interface{}) {
	link.value = value
}

// Get the link's prev.
func (link *Link) GetPrev() *Link {
	return link.prev
}

// Get the link's next.
func (link *Link) GetNext() *Link {
	return link.next
}

// Remove the link that calls PopSelf() from its list.
/*
Cases to consider:
- If PopSelf() is called by the only link in a list
- If PopSelf() is called by the tail link in a list
- If PopSelf() is called by the head link in a list
- If PopSelf() is called by a link in the middle of a list
*/
func (link *Link) PopSelf() {
	if link.prev == nil && link.next == nil {
		link.list.head = nil
		link.list.tail = nil
		link.list = nil
	} else if link.prev == nil {
		link.next.prev = nil
		link.list.head = link.next
		link.list = nil
		link.next = nil
	} else if link.next == nil {
		link.prev.next = nil
		link.list.tail = link.prev
		link.list = nil
		link.prev = nil
	} else {
		prevlink := link.prev
		prevlink.next = link.next
		link.prev.next = link.next
		link.next.prev = prevlink
		link.list = nil
		link.next = nil
		link.prev = nil
	}
}
