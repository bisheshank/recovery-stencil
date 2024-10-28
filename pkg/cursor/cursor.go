package cursor

import (
	"dinodb/pkg/entry"
)

// Interface for a cursor that traverses a table.
type Cursor interface {
	Next() bool                     //Moves the cursor to the next entry in the index
	GetEntry() (entry.Entry, error) //Returns the entry at the position of the cursor
	Close()                         //Called to indicate that the cursor is done being used
}
