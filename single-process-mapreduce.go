// Package main demonstrates a minimal, in-memory implementation of the
// MapReduce programming model, originally described by Dean and Ghemawat
// (2004).
//
// The program runs the three canonical MapReduce phases in sequence:
//
//  1. Map     – mapWords splits the input text into individual words.
//  2. Shuffle – shuffle groups the emitted words into per-key buckets.
//  3. Reduce  – reduce counts the occurrences of each word.
//
// Everything runs in a single process. A real MapReduce runtime would
// distribute these phases across many machines and shuffle data over the
// network, but the shape of the computation is identical.
package main

import (
	"fmt"
	"strings"
)

// mapWords is the Map phase.
//
// It applies a map function to the input text and emits an intermediate
// collection of words. Here the input is a single string and the emitted
// records are the whitespace-separated fields it contains. In the MapReduce
// model this is the step that runs in parallel over many input splits.
//
// For example, mapWords("cat dog cat") returns ["cat", "dog", "cat"].
func mapWords(input string) []string {
	return strings.Fields(input)
}

// shuffle is the Shuffle phase.
//
// It collects the words emitted by the Map phase and groups them by key
// (the word itself). Each occurrence is appended as a value of 1, so the
// resulting map holds one entry per distinct word whose slice length equals
// that word's frequency. This models the "shuffle and sort" step where the
// runtime routes every intermediate record to the reducer responsible for
// its key.
//
// For example, shuffle(["cat", "dog", "cat"]) returns
// {"cat": [1, 1], "dog": [1]}.
func shuffle(words []string) map[string][]int {
	buckets := make(map[string][]int)

	for _, word := range words {
		buckets[word] = append(buckets[word], 1)
	}

	return buckets
}

// reduce is the Reduce phase.
//
// It consumes the key-grouped buckets produced by the Shuffle phase and
// applies a reduce function to the values of each key. Here the reduce
// function sums the values, so each key maps to the number of times it
// occurred. This is the classic MapReduce word-count aggregation.
//
// For example, reduce({"cat": [1, 1], "dog": [1]}) returns
// {"cat": 2, "dog": 1}.
func reduce(buckets map[string][]int) map[string]int {
	result := make(map[string]int)

	for key, values := range buckets {
		sum := 0

		for _, value := range values {
			sum += value
		}

		result[key] = sum
	}

	return result
}

// main wires the three MapReduce phases together and prints the result of
// each one for the hard-coded sample input.
func main() {
	input := "cat dog cat dog cat"

	// Map: split the input into individual words.
	words := mapWords(input)
	fmt.Println("Map:", words)

	// Shuffle: group the words by key into buckets of 1s.
	buckets := shuffle(words)
	fmt.Println("Shuffle:", buckets)

	// Reduce: sum each bucket to obtain the word counts.
	result := reduce(buckets)
	fmt.Println("Reduce:", result)
}
