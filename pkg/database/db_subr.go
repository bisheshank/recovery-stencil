package database

import (
	"os"
)

// GetTempDB Creates and returns the name of a temporary .db file used to back a pager.
func GetTempDB() (string, error) {
	tmpfile, err := os.CreateTemp("", "*.db")
	if err != nil {
		return "", err
	}
	_ = tmpfile.Close()
	return tmpfile.Name(), nil
}
