package go_test

import (
	"fmt"
	"strings"
	"testing"

	"dinodb/pkg/repl"
)

func f1(s string, _ *repl.REPLConfig) (string, error) { return "", nil }
func f2(s string, _ *repl.REPLConfig) (string, error) { return "", nil }
func f3(s string, _ *repl.REPLConfig) (string, error) { return "", nil }
func f4(s string, _ *repl.REPLConfig) (string, error) { return "", nil }
func f5(s string, _ *repl.REPLConfig) (string, error) { return "", nil }

func TestRepl(t *testing.T) {
	t.Run("NewRepl", testNewRepl)
	t.Run("Add", testAdd)
	t.Run("HelpString", testHelpString)
	t.Run("CombineZeroRepl", testCombineZeroRepl)
}

// Tests that a newly REPL doesn’t contain any commands other than the metacommands.
func testNewRepl(t *testing.T) {
	r := repl.NewRepl()
	commands := r.GetCommands()
	for k := range commands {
		t.Fatal("commands should be empty; found key:", k)
	}
	help := r.GetHelp()
	for k := range help {
		t.Fatal("commands should be empty; found key:", k)
	}
}

/*
Tests that commands and help strings can be properly accessed
upon adding commands to a new REPL.
*/
func testAdd(t *testing.T) {
	r := repl.NewRepl()
	r.AddCommand("1", f1, "1 help")
	r.AddCommand("2", f2, "2 help")
	r.AddCommand("3", f3, "3 help")
	r.AddCommand("4", f4, "4 help")
	r.AddCommand("5", f5, "5 help")
	if _, ok := r.GetCommands()["1"]; !ok {
		t.Fatal("bad add command")
	}
	if _, ok := r.GetCommands()["2"]; !ok {
		t.Fatal("bad add command")
	}
	if _, ok := r.GetCommands()["3"]; !ok {
		t.Fatal("bad add command")
	}
	if _, ok := r.GetCommands()["4"]; !ok {
		t.Fatal("bad add command")
	}
	if _, ok := r.GetCommands()["5"]; !ok {
		t.Fatal("bad add command")
	}
	if _, ok := r.GetHelp()["1"]; !ok {
		t.Fatal("bad add help")
	}
	if _, ok := r.GetHelp()["2"]; !ok {
		t.Fatal("bad add help")
	}
	if _, ok := r.GetHelp()["3"]; !ok {
		t.Fatal("bad add help")
	}
	if _, ok := r.GetHelp()["4"]; !ok {
		t.Fatal("bad add help")
	}
	if _, ok := r.GetHelp()["5"]; !ok {
		t.Fatal("bad add help")
	}
}

// Tests the validity of the help strings added to commands.
func testHelpString(t *testing.T) {
	r := repl.NewRepl()
	r.AddCommand("1", f1, "1 help")
	r.AddCommand("2", f2, "2 help")
	r.AddCommand("3", f3, "3 help")
	r.AddCommand("4", f4, "4 help")
	r.AddCommand("5", f5, "5 help")
	if !strings.Contains(r.HelpString(), "1 help") {
		t.Fatal("bad print help")
	}
	if !strings.Contains(r.HelpString(), "2 help") {
		t.Fatal("bad print help")
	}
	if !strings.Contains(r.HelpString(), "3 help") {
		t.Fatal("bad print help")
	}
	if !strings.Contains(r.HelpString(), "4 help") {
		t.Fatal("bad print help")
	}
	if !strings.Contains(r.HelpString(), "5 help") {
		t.Fatal("bad print help")
	}
}

// Tests that combining multiple empty REPLs still gives you an empty REPL
func testCombineZeroRepl(t *testing.T) {
	r, err := repl.CombineRepls([]*repl.REPL{})
	if err != nil {
		t.Fatal("bad combine")
	}
	if len(r.GetCommands()) != 0 {
		t.Fatal("bad combine - should not have any commands")
	}
	if len(r.GetHelp()) != 0 {
		t.Fatal("bad combine - should not have any commands")
	}
}

func TestReplRun(t *testing.T) {
	t.Run("EmptyHelp", testRunEmptyHelp)
	t.Run("InvalidCommand", testRunInvalidCommand)
	t.Run("SingleCommand", testRunSingleCommand)
	t.Run("CannotOverwriteHelp", testRunCannotOverwriteHelpCommand)
	t.Run("Prompt", testRunPrompt)
}

func testRunEmptyHelp(t *testing.T) {
	r := repl.NewRepl()
	input, output := startRepl(t, r)

	fmt.Fprintln(input, ".help")
	checkOutputExact(t, output, "")
}

func testRunInvalidCommand(t *testing.T) {
	r := repl.NewRepl()
	input, output := startRepl(t, r)

	fmt.Fprintln(input, "invalid")
	checkOutputHasErrorMessage(t, output, repl.ErrCommandNotFound)
}

func echo(s string, r *repl.REPLConfig) (output string, err error) {
	return s, nil
}

func testRunSingleCommand(t *testing.T) {
	r := repl.NewRepl()
	r.AddCommand("echo", echo, "prints back everything")
	input, output := startRepl(t, r)

	// Check running the command produces expected output
	fmt.Fprintln(input, "echo hey")
	checkOutputExact(t, output, "echo hey\n")
}

func testRunCannotOverwriteHelpCommand(t *testing.T) {
	r := repl.NewRepl()
	r.AddCommand("echo", echo, "prints back everything")
	r.AddCommand(".help", f1, "fake help")
	input, output := startRepl(t, r)

	checkHelp(t, input, output, map[string]string{"echo": "prints back everything"})
}

func testRunPrompt(t *testing.T) {
	r := repl.NewRepl()
	prompt := "> "
	r.AddCommand("1", f1, "f1 help")
	input, output := startReplWithPrompt(t, r, prompt)

	fmt.Fprintln(input, "1")
	nextOutput := getAllOutput(output)
	if !strings.HasPrefix(nextOutput, prompt) {
		t.Fatal("Prompt was missing from output")
	}
}
