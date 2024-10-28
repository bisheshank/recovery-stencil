// Global database config.
package config

// Name of the database.
const DBName = "dinodb"

// Prompt printed by REPL.
const Prompt = DBName + "> "

// The maximum number of pages that can be in the pager's buffer at once.
const MaxPagesInBuffer = 32

// Name of log file.
const LogFileName = "db.log"

// Return prompt if requested, else "".
func GetPrompt(flag bool) string {
	if flag {
		return Prompt
	}
	return ""
}
