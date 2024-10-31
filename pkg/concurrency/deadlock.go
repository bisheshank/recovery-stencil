package concurrency

import (
	"errors"
	"sync"
)

// WaitsForGraph is a precedence graph used to keep track of whether
// there are deadlocks in transactions
type WaitsForGraph struct {
	edges []Edge       // A slice of all the Edges that we have in our graph
	mtx   sync.RWMutex // Mutex for synchronizing access to the edges slice.
}

// An Edge between transactions in a ("waits-for") Graph
// if Txn1 is waiting for a resource held by Txn2,
// then there is an Edge from Txn1 to Txn2
type Edge struct {
	from *Transaction
	to   *Transaction
}

func NewGraph() *WaitsForGraph {
	return &WaitsForGraph{edges: make([]Edge, 0)}
}

// Add an edge from `from` to `to`. Logically, `from` waits for `to`.
func (g *WaitsForGraph) AddEdge(from *Transaction, to *Transaction) {
	g.mtx.Lock()
	defer g.mtx.Unlock()
	g.edges = append(g.edges, Edge{from: from, to: to})
}

// Remove an edge. Only removes one of these edges if multiple copies exist.
func (g *WaitsForGraph) RemoveEdge(from *Transaction, to *Transaction) error {
	g.mtx.Lock()
	defer g.mtx.Unlock()
	toRemove := Edge{from: from, to: to}
	for i, e := range g.edges {
		if e == toRemove {
			g.edges = removeHelper(g.edges, i)
			return nil
		}
	}
	return errors.New("edge not found")
}

// Remove the element at index `i` from `list`.
func removeHelper(list []Edge, i int) []Edge {
	list[i] = list[len(list)-1]
	return list[:len(list)-1]
}

// Return true if a cycle exists; false otherwise.
func (g *WaitsForGraph) DetectCycle() (hasCycle bool) {
	g.mtx.RLock()
	defer g.mtx.RUnlock()
	/* SOLUTION {{{ */
	// Get all transactions in the graph, using a set to avoid duplicates
	tSet := make(map[*Transaction]bool)
	for _, e := range g.edges {
		tSet[e.from] = true
		tSet[e.to] = true
	}

	for k := range tSet {
		if dfs(g, k, make(map[*Transaction]bool)) {
			return true
		}
	}

	return false
	/* SOLUTION }}} */
}

// depth-first search function to help detect cycles in a graph
func dfs(g *WaitsForGraph, from *Transaction, seen map[*Transaction]bool) bool {
	// Go through each edge.
	for _, e := range g.edges {
		// If there is an edge from here to elsewhere,
		if e.from == from {
			// Check if it creates a cycle.
			if _, ok := seen[e.to]; ok {
				return ok
			}
			// Otherwise, run dfs on it.
			seen[e.to] = true
			return dfs(g, e.to, seen)
		}
	}
	return false
}
