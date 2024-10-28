package repl

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/google/uuid"
)

type ReplCommand func(string, *REPLConfig) (output string, err error)

const (
	// Trigger for the help meta-command that prints out all help strings
	TriggerHelpMetacommand = ".help"

	// String that should be prepended to any error before being sent to the output writer
	ErrorPrependStr = "ERROR: "
)

var (
	// use in combine repls function
	ErrOverlappingCommands = errors.New("found overlapping")

	// Error for when a sent trigger is not associated with any known commands
	ErrCommandNotFound = errors.New("command not found")
)

// REPL struct.
type REPL struct {
	commands map[string]ReplCommand
	help     map[string]string
}

// REPL Config struct.
type REPLConfig struct {
	clientId uuid.UUID
}

// Get address.
func (replConfig *REPLConfig) GetAddr() uuid.UUID {
	return replConfig.clientId
}

// Construct an empty REPL.
// When a new REPL is created, its commands should be empty.
func NewRepl() *REPL {
	/* SOLUTION {{{ */
	return &REPL{make(map[string]ReplCommand),
		make(map[string]string)}
	/* SOLUTION }}} */
}

// helper function for contain
func contains(s []string, str string) bool {
	for _, v := range s {
		if v == str {
			return true
		}
	}

	return false
}

// Combines a slice of REPLs.
/*
	- Error if the REPLs being combined have any overlapping commands (same trigger).
	- If no REPLs are given, return a new empty REPL.
*/
func CombineRepls(repls []*REPL) (*REPL, error) {
	/* SOLUTION {{{ */
	if len(repls) == 0 {
		return NewRepl(), nil
	} else {
		newrepl := NewRepl()
		var listexist []string
		for i := 0; i < len(repls); i++ {
			for key, value := range repls[i].commands {
				if contains(listexist, key) {
					return nil, ErrOverlappingCommands
				} else {
					newrepl.AddCommand(key, value, repls[i].help[key])
					listexist = append(listexist, key)
				}
			}
		}
		return newrepl, nil
	}
	/* SOLUTION }}} */
}

// Get commands.
func (r *REPL) GetCommands() map[string]ReplCommand {
	return r.commands
}

// Get help.
func (r *REPL) GetHelp() map[string]string {
	return r.help
}

// Add a command, along with its help string, to the set of commands.
/*
	-	if the given command already exists (duplicate trigger given),
		overwrite the previous command with what is given
*/
func (r *REPL) AddCommand(trigger string, action ReplCommand, help string) {
	if trigger == TriggerHelpMetacommand {
		return // TODO: return error
	}
	r.commands[trigger] = action
	r.help[trigger] = help
}

// Return all REPL commands' help strings as one string
func (r *REPL) HelpString() string {
	var sb strings.Builder
	for k, v := range r.help {
		sb.WriteString(fmt.Sprintf("%s: %s\n", k, v))
	}
	return sb.String()
}

/*
Writes the welcome string and then runs the REPL loop.
- Get and process the input.
- If the trigger is '.help', write the REPL's HelpString() out.
- If the trigger is not '.help',
  - If the command exists, run the command with the input and display results in output.
  - if the command doesn't exist, display a command not found string to output.

- Repeat

[Notes]:
- 'prompt' is the prefix at the beginning of lines showing that the REPL is ready to accept input
  - ex: If the REPL line is 'dinodb>          ', 'dinodb>' would be the prompt

- Note that input and output default to Stdin and Stdout if not specified
- Check out the cleanInput() function to clean user input.
- Explore the documentation for bufio.Scanner, io.WriteString(), strings.Fields()
- You should pass the entire payload string to the first parameter in action when a command is run. Don’t remove 
the equivalent of argv[0] - pass the whole string! 
*/
func (r *REPL) Run(clientId uuid.UUID, prompt string, input io.Reader, output io.Writer) {
	// Set input and writer to stdin and stdout if left unspecified
	if input == nil {
		input = os.Stdin
	}
	if output == nil {
		output = os.Stdout
	}

	scanner := bufio.NewScanner(input)
	replConfig := &REPLConfig{clientId: clientId}
	// Make sure to write messages to `output` and not stdout! This means using functions like
	// io.WriteString(output, ...) and fmt.Fprintln(output, ...) instead of fmt.Println(...) for your REPL
	fmt.Fprintln(output, "Welcome to the dinodb REPL! Please type '.help' to see the list of available commands.")
	io.WriteString(output, prompt)

	// Begin the repl loop!
	for scanner.Scan() {
		/* SOLUTION {{{ */
		payload := scanner.Text()
		fields := strings.Fields(payload)
		if len(fields) == 0 {
			io.WriteString(output, prompt)
			continue
		}
		trigger := fields[0]

		// Check for the help meta-command.
		if trigger == TriggerHelpMetacommand {
			io.WriteString(output, r.HelpString())
			io.WriteString(output, prompt)
			continue
		}

		// Else, check user-specified commands.
		if command, exists := r.commands[trigger]; exists {
			result, err := command(payload, replConfig)
			if err != nil {
				fmt.Fprintf(output, "%s%s\n", ErrorPrependStr, err)
			} else {
				// Append newline if there is output and if it doesn't end with a newline already
				if len(result) != 0 && !strings.HasSuffix(result, "\n") {
					result = result + "\n"
				}

				io.WriteString(output, result)
			}
		} else {
			fmt.Fprintf(output, "%s%s\n", ErrorPrependStr, ErrCommandNotFound)
		}
		io.WriteString(output, prompt)
		/* SOLUTION }}} */
	}
	// Print an additional line if we encountered an EOF character.
	io.WriteString(output, "\n")
}

// Run the REPL.
/*
	Ignore until Concurrency
*/
func (r *REPL) RunChan(c chan string, clientId uuid.UUID, prompt string) {
	// Get reader and writer; stdin and stdout if no conn.
	writer := os.Stdout
	replConfig := &REPLConfig{clientId: clientId}
	// Begin the repl loop!
	io.WriteString(writer, prompt)
	for payload := range c {
		// Emit the payload for debugging purposes.
		io.WriteString(writer, payload+"\n")
		// Parse the payload.
		fields := strings.Fields(payload)
		if len(fields) == 0 {
			io.WriteString(writer, prompt)
			continue
		}
		trigger := fields[0]
		// Check for a meta-command.
		if trigger == ".help" {
			io.WriteString(writer, r.HelpString())
			io.WriteString(writer, prompt)
			continue
		}
		// Else, check user commands.
		if command, exists := r.commands[trigger]; exists {
			// Call a hardcoded function.
			result, err := command(payload, replConfig)
			if err != nil {
				io.WriteString(writer, fmt.Sprintf("%v\n", err))
			} else {
				io.WriteString(writer, fmt.Sprintln(result))
			}
		} else {
			io.WriteString(writer, ErrCommandNotFound.Error())
		}
		io.WriteString(writer, prompt)
	}
	// Print an additional line if we encountered an EOF character.
	io.WriteString(writer, "\n")
}
