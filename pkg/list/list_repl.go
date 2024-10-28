package list

import (
	"errors"
	"fmt"
	"strings"

	"dinodb/pkg/repl"
)

// Use these provided errors instead of defining your own!
var (
	ErrListPrintInvalidArgs = errors.New("invalid arguments, usage: list_print")

	ErrListPushHeadInvalidArgs = errors.New("invalid arguments, usage: list_push_head <elt>")

	ErrListPushTailInvalidArgs = errors.New("invalid arguments, usage: list_push_tail <elt>")

	ErrListRemoveValueNotFound = errors.New("link with given value was not found")
	ErrListRemoveInvalidArgs   = errors.New("invalid arguments, usage: list_remove <elt>")

	ErrListContainsInvalidArgs = errors.New("invalid arguments, usage: list_contains <elt>")
)

const (
	// Use these help strings for each command instead of defining your own!
	HelpListPrint    = "Input: List of anything. Prints out all of the elements in the list in order. usage: list_print"
	HelpListPushHead = "Inserts the given element to the head of the list as a string. usage: list_push_head <elt>"
	HelpListPushTail = "Inserts the given element to the end of the list as a string. usage: list_push_tail <elt>"
	HelpListRemove   = "Removes the given element from the list. usage: list_remove <elt>"
	HelpListContains = "Check whether the element is in the list or not. usage: list_contains <elt>"

	// Output strings for the list_contains command
	OutputListContainsFound    = "value was found"
	OutputListContainsNotFound = "value was not found"
)

/*
Create a NewRepl() and use repl.AddCommand() to create these following commands:
- list_print
- list_push_head <elt>
- list_push_tail <elt>
- list_remove <elt>
- list_contains <elt>

[Notes]:
Remember that AddCommand() takes in a:

1. Trigger (name of the command),

2. ReplCommand (function containing code to run command),
  - A ReplCommand is a function that returns a string and error (string, error).
  - Return any output in the string, and any errors in error
  - We check error based on whether it is nil or not
  - If an error exists, the returned string is NOT used! Instead, return an error (using the above defined INVALID_ARGS_ERR)
  - There are also custom error checks for some functions. Hint: They should match the above defined error messages

3. Help string (string explaining how to use the command)
  - Help strings should be one line! Do NOT use '\n' in the Help Strings.
  - Use the above help str vars for your help strings!
*/
func ListRepl(list *List) *repl.REPL {
	// SOLUTION {{{
	newrepl := repl.NewRepl()

	newrepl.AddCommand("list_print", func(payload string, replConfig *repl.REPLConfig) (string, error) {
		if len(strings.Split(payload, " ")) == 1 {
			printBuilder := new(strings.Builder)
			list.Map(func(linkput *Link) { fmt.Fprintln(printBuilder, linkput.value) })
			return printBuilder.String(), nil
		} else {
			return "", ErrListPrintInvalidArgs
		}
	}, HelpListPrint)

	newrepl.AddCommand("list_push_head", func(payload string, replConfig *repl.REPLConfig) (string, error) {
		if tokens := strings.Split(payload, " "); len(tokens) == 2 {
			list.PushHead(tokens[1])
			return "", nil
		} else {
			return "", ErrListPushHeadInvalidArgs
		}
	}, HelpListPushHead)

	newrepl.AddCommand("list_push_tail", func(payload string, replConfig *repl.REPLConfig) (string, error) {
		if tokens := strings.Split(payload, " "); len(tokens) == 2 {
			list.PushTail(tokens[1])
			return "", nil
		} else {
			return "", ErrListPushTailInvalidArgs
		}
	}, HelpListPushTail)

	newrepl.AddCommand("list_remove", func(payload string, replConfig *repl.REPLConfig) (string, error) {
		if len(strings.Split(payload, " ")) == 2 {
			link_to_remove := list.Find(func(linkfind *Link) bool { return linkfind.value == strings.Split(payload, " ")[1] })
			if link_to_remove != nil {
				link_to_remove.PopSelf()
				return "", nil
			} else {
				return "", ErrListRemoveValueNotFound
			}
		} else {
			return "", ErrListRemoveInvalidArgs
		}
	}, HelpListRemove)

	newrepl.AddCommand("list_contains", func(payload string, replConfig *repl.REPLConfig) (string, error) {
		if len(strings.Split(payload, " ")) == 2 {
			if list.Find(func(linkfind *Link) bool { return linkfind.value == strings.Split(payload, " ")[1] }) != nil {
				return OutputListContainsFound, nil
			} else {
				return OutputListContainsNotFound, nil
			}
		} else {
			return "", ErrListContainsInvalidArgs
		}
	}, HelpListContains)

	return newrepl
	// SOLUTION }}}
}
