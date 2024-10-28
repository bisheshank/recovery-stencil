package concurrency

import (
	"sync"

	"github.com/google/uuid"
)

// Each client will have at most one transaction running at a given time.
// Therefore, the clientID is a unique identifier for both the Transaction and its Client
type Transaction struct {
	clientId        uuid.UUID
	lockedResources map[Resource]LockType 	// tracks currently locked resources and LockType. Useful for error handling when Locking
	mtx             sync.RWMutex
}

func (t *Transaction) WLock() {
	t.mtx.Lock()
}

func (t *Transaction) WUnlock() {
	t.mtx.Unlock()
}

func (t *Transaction) RLock() {
	t.mtx.RLock()
}

func (t *Transaction) RUnlock() {
	t.mtx.RUnlock()
}

func (t *Transaction) GetClientID() (clientId uuid.UUID) {
	return t.clientId
}

func (t *Transaction) GetResources() (resources map[Resource]LockType) {
	return t.lockedResources
}
