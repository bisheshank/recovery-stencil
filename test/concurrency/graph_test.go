package concurrency_test

import (
	"dinodb/pkg/concurrency"
	"testing"
)

func TestDeadlock(t *testing.T) {
	t.Run("Empty", testDeadlockEmpty)
	t.Run("OneEdge", testDeadlockOneEdge)
	t.Run("Simple", testDeadlockSimple)
	t.Run("DAGSmall", testDeadlockDAGSmall)
}

func testDeadlockEmpty(t *testing.T) {
	g := concurrency.NewGraph()
	if g.DetectCycle() {
		t.Error("cycle detected in empty graph")
	}
}

func testDeadlockOneEdge(t *testing.T) {
	t1 := concurrency.Transaction{}
	t2 := concurrency.Transaction{}
	g := concurrency.NewGraph()
	g.AddEdge(&t1, &t2)
	if g.DetectCycle() {
		t.Error("cycle detected in one edge graph")
	}
}

func testDeadlockSimple(t *testing.T) {
	t1 := concurrency.Transaction{}
	t2 := concurrency.Transaction{}
	g := concurrency.NewGraph()
	g.AddEdge(&t1, &t2)
	g.AddEdge(&t2, &t1)
	if !g.DetectCycle() {
		t.Error("failed to detect cycle")
	}
}

func testDeadlockDAGSmall(t *testing.T) {
	t1 := concurrency.Transaction{}
	t2 := concurrency.Transaction{}
	g := concurrency.NewGraph()
	g.AddEdge(&t1, &t2)
	g.AddEdge(&t1, &t2)
	if g.DetectCycle() {
		t.Error("cycle detected in DAG")
	}
}
