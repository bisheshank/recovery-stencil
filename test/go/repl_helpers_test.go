package go_test

// This file contains helper functions used for testing the first assignment.
// In order to be in the same package as the tests, it's name must end with "_test"
// (even though this file contains no tests)

import (
	"dinodb/pkg/repl"
	"dinodb/test/utils"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

const replTimeout = 10 * time.Millisecond // Time window for reading repl output
const outputChannelBufferSize = 10_000    // Arbitrarily large to prevent blocking when writing to repl output

func startRepl(t *testing.T, r *repl.REPL) (input io.Writer, output <-chan string) {
	return startReplWithPrompt(t, r, "")
}

// Starts a repl with a custom prompt, returning the writer
// to write input to and the output channel that output is written to
func startReplWithPrompt(t *testing.T, r *repl.REPL, prompt string) (input io.Writer, output <-chan string) {
	// setup pipes for communicating with the REPL
	// TODO: change to use channels so we can timeout if repl doesn't read from input (will block our tests currently)
	inputPReader, inputPWriter := io.Pipe()
	utils.EnsureCleanup(t, func() {
		_ = inputPWriter.Close()
	})
	outputPReader, outputPWriter := io.Pipe()

	// Run repl in separate goroutine (since Run blocks as it is waiting for input)
	go func() {
		r.Run(uuid.New(), prompt, inputPReader, outputPWriter)
		_ = outputPWriter.Close()
	}()

	// Create buffered channel for sending repl output to
	outputCh := make(chan string, outputChannelBufferSize)

	// Continuously copy the repl output from the pipe to the output channel,
	// only terminating once the pipe closes
	go func() {
		for {
			buf := make([]byte, 1_000)
			n, err := outputPReader.Read(buf)
			if n != 0 {
				outputCh <- string(buf[:n])
			}
			if err != nil {
				break
			}
		}
		close(outputCh)
	}()

	// Skip the welcome message / any other initialization output
	_ = getAllOutput(outputCh)
	return inputPWriter, outputCh
}

// Returns all the output that has been written to outputCh before the timeout
func getAllOutput(outputCh <-chan string) string {
	timeoutTimer := time.NewTimer(replTimeout)
	sb := new(strings.Builder)
	for {
		select {
		case outputLine := <-outputCh:
			sb.WriteString(outputLine)
		case <-timeoutTimer.C:
			return sb.String()
		}
	}
}

// Checks that the output sent to the output channel exactly matches a string
func checkOutputExact(t *testing.T, outputCh <-chan string, expected string) {
	result := getAllOutput(outputCh)
	if result != expected {
		t.Fatalf("Expected %q as the output, but found %q", expected, result)
	}
}

// Checks that only an error message has been written to output
func checkOutputHasErrorMessage(t *testing.T, outputCh <-chan string, err error) {
	checkOutputExact(t, outputCh, fmt.Sprintf("%s%s\n", repl.ErrorPrependStr, err))
}

// Checks that the repl's .help command prints help messages corresponding to the helpMap,
// which maps command trigger to help message
func checkHelp(t *testing.T, input io.Writer, outputCh <-chan string, helpMap map[string]string) {
	fmt.Fprintln(input, repl.TriggerHelpMetacommand)
	helpOutput := getAllOutput(outputCh)
	for cmd, helpMsg := range helpMap {
		expectedHelp := fmt.Sprintf("%s: %s\n", cmd, helpMsg)
		if !strings.Contains(helpOutput, expectedHelp) {
			t.Errorf("Didn't find expected help string %q for command %q", helpMsg, cmd)
		}
	}

	if numLines := strings.Count(helpOutput, "\n"); len(helpMap) != numLines {
		t.Errorf("Expected help string to have one line for each of %d registered commands, but instead found %d lines", len(helpMap), numLines)
	}

	if t.Failed() {
		t.FailNow()
	}
}
