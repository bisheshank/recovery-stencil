package concurrency

import (
	"errors"
	"sync"
)

// ResourceLockManager handles the locking of database resources.
type ResourceLockManager struct {
	locks map[Resource]*sync.RWMutex
	mtx   sync.Mutex
}

func NewResourceLockManager() *ResourceLockManager {
	return &ResourceLockManager{
		locks: make(map[Resource]*sync.RWMutex),
	}
}

// Lock the resource in the database (read lock or write lock depending on `lType`)
func (lm *ResourceLockManager) Lock(r Resource, lType LockType) error {
	// Safely acquire the mutex guarding the Resource, initializing the mutex if needed
	lm.mtx.Lock()
	lock, found := lm.locks[r]
	if !found {
		lm.locks[r] = &sync.RWMutex{}
		lock = lm.locks[r]
	}
	lm.mtx.Unlock()
	// Lock accordingly
	switch lType {
	case R_LOCK:
		lock.RLock()
	case W_LOCK:
		lock.Lock()
	}
	return nil
}

// Unlock the resource in the database (read unlock or write unlock depending on `lType`)
func (lm *ResourceLockManager) Unlock(r Resource, lType LockType) error {
	// Safely acquire the mutex guarding the Resource
	lm.mtx.Lock()
	lock, found := lm.locks[r]
	if !found {
		return errors.New("tried to unlock nonexistent resource")
	}
	lm.mtx.Unlock()
	// Unlock accordingly
	switch lType {
	case R_LOCK:
		lock.RUnlock()
	case W_LOCK:
		lock.Unlock()
	}
	return nil
}
