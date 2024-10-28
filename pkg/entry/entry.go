package entry

import (
	"encoding/binary"
	"fmt"
	"io"
)

// Entry is a key-value pair that is usually used to represent an entry in a BTree or Hash table.
type Entry struct {
	Key   int64
	Value int64
}

// New constructs and returns a new Entry with the specified key and value.
func New(key int64, value int64) Entry {
	return Entry{key, value}
}

// Marshal serializes a given entry into a byte array.
func (entry Entry) Marshal() []byte {
	// Marshall the key field.
	var newdata []byte
	bin := make([]byte, binary.MaxVarintLen64)
	binary.PutVarint(bin, entry.Key)
	newdata = bin
	// Marshall the value field.
	bin = make([]byte, binary.MaxVarintLen64)
	binary.PutVarint(bin, entry.Value)
	newdata = append(newdata, bin...)
	// Return the combined byte array.
	return newdata
}

// UnmarshalEntry deserializes a byte array into an entry.
func UnmarshalEntry(data []byte) Entry {
	k, _ := binary.Varint(data[:len(data)/2])
	v, _ := binary.Varint(data[len(data)/2:])
	return Entry{Key: k, Value: v}
}

// Print writes the entry to the specified writer in the following format: (<key>, <value>)
func (entry Entry) Print(w io.Writer) {
	fmt.Fprintf(w, "(%d, %d), ", entry.Key, entry.Value)
}
