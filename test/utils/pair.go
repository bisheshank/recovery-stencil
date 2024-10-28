package utils

import "math/rand"

// KeyValuePair is a pair of key and value int64s
type KeyValuePair struct {
	Key int64
	Val int64
}

// GenerateRandomKeyValuePairs generates n random key-value pairs with unique keys.
// Returns the n pairs generated in a slice and a map that maps the generated keys to the generated values.
func GenerateRandomKeyValuePairs(n int64) ([]KeyValuePair, map[int64]int64) {
	entries := make([]KeyValuePair, n)
	answerKey := make(map[int64]int64, n)
	for i := range n {
	genKey:
		key := rand.Int63()
		if _, ok := answerKey[key]; ok {
			goto genKey
		}
		val := rand.Int63()
		answerKey[key] = val
		entries[i] = KeyValuePair{Key: key, Val: val}
	}
	return entries, answerKey
}
