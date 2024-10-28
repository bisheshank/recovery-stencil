package go_test

import (
	"fmt"
	"io"
	"testing"

	"dinodb/pkg/list"
)

func TestListRepl(t *testing.T) {
	t.Run("Help", testListReplHelp)
	t.Run("AddHead", testListReplAddHead)
	t.Run("Print", testListReplPrint)
	t.Run("Contains", testListReplContains)
}

// Helper function for starting the List REPL and getting input / output streams
func startListRepl(t *testing.T) (input io.Writer, outputCh <-chan string) {
	// setup repl
	l := list.NewList()
	studentRepl := list.ListRepl(l)
	return startRepl(t, studentRepl)
}

// Checks that a successful command doesn't write anything to output
func checkSuccessOutput(t *testing.T, outputCh <-chan string, cmdName string) {
	output := getAllOutput(outputCh)
	if output != "" {
		t.Fatalf("Successful %s commands should not have any output, but instead found output %q", cmdName, output)
	}
}

// Tests that the ListREPL can be created successfully
// and that the help string has all the correct lines
func testListReplHelp(t *testing.T) {
	helpMap := map[string]string{
		"list_print":     list.HelpListPrint,
		"list_push_head": list.HelpListPushHead,
		"list_push_tail": list.HelpListPushTail,
		"list_remove":    list.HelpListRemove,
		"list_contains":  list.HelpListContains,
	}
	input, output := startListRepl(t)

	checkHelp(t, input, output, helpMap)
}

// Tests successful and failed list_push_head calls
func testListReplAddHead(t *testing.T) {
	inputWriter, output := startListRepl(t)

	//successful list_push_head
	io.WriteString(inputWriter, "list_push_head 1\n")
	checkSuccessOutput(t, output, "list_push_head")

	//ill-formed list_push_head
	io.WriteString(inputWriter, "list_push_head\n")
	checkOutputHasErrorMessage(t, output, list.ErrListPushHeadInvalidArgs)
}

// Tests form of list_print calls
func testListReplPrint(t *testing.T) {
	inputWriter, output := startListRepl(t)

	//call list_print on empty list
	io.WriteString(inputWriter, "list_print\n")
	checkOutputExact(t, output, "")

	//push a link onto head of list
	io.WriteString(inputWriter, "list_push_head 1\n")

	//call list_print again and check for valid response
	io.WriteString(inputWriter, "list_print\n")
	checkOutputExact(t, output, "1\n")
}

// Tests successful and failed list_contains calls
func testListReplContains(t *testing.T) {
	inputWriter, output := startListRepl(t)

	//add an element to list
	io.WriteString(inputWriter, "list_push_head 1\n")

	//ill-formed list_contains call
	io.WriteString(inputWriter, "list_contains\n")
	checkOutputHasErrorMessage(t, output, list.ErrListContainsInvalidArgs)

	//list_contains call on existing link
	io.WriteString(inputWriter, "list_contains 1\n")
	checkOutputExact(t, output, fmt.Sprintln(list.OutputListContainsFound))

	//list_contains call on non-existent link
	io.WriteString(inputWriter, "list_contains 2\n")
	checkOutputExact(t, output, fmt.Sprintln(list.OutputListContainsNotFound))
}
