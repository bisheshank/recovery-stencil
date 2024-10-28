package concurrency

// Indicates whether a lock is a reader or a writer lock.
type LockType int

const (
	R_LOCK LockType = 0
	W_LOCK LockType = 1
)

// A Resource refers to an entry in our database,
// uniquely identified by tableName and key
type Resource struct {
	tableName string
	key       int64
}

func (r *Resource) GetTableName() string {
	return r.tableName
}

func (r *Resource) GetResourceKey() int64 {
	return r.key
}
