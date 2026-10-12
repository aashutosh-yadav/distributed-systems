// Package main demonstrates the same in-memory MapReduce word count as
// single-process-mr.go, but this time the Map phase runs concurrently: each
// input split is mapped in its own goroutine.
//
// This is a first step toward the real MapReduce runtime, where map work is
// spread across many machines. Here it is spread across many goroutines
// inside a single process.
//
// Phases:
//
//  1. Map     – one goroutine per input split, all running at the same time.
//  2. Shuffle – group the emitted words into per-key buckets.
//  3. Reduce  – count the occurrences of each word.
package main

import (
	"fmt"
	"strings"
	"sync"
)

// mapWords is the Map phase. converts the input string into a slice.
func mapWords(input string) []string {
	return strings.Fields(input)
}

// shuffle groups the emitted words by key into buckets of 1s.
func shuffle(words []string) map[string][]int {
	buckets := make(map[string][]int)

	for _, word := range words {
		buckets[word] = append(buckets[word], 1)
	}

	fmt.Println("this is shuffel ", buckets)
	return buckets
}

// reduce sums each bucket to obtain the word counts.
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

// concurrentMap runs mapWords on every input split at the same time, one
// goroutine per split, and returns all the emitted words.
//
// The goroutines send their words on a shared channel. A separate goroutine
// closes the channel once every map goroutine has finished (tracked with a
// WaitGroup), so the range loop ends cleanly. No locks are needed: the channel
// serialises the concurrent sends.
func concurrentMap(inputs []string) []string {
	var wg sync.WaitGroup
	wordCh := make(chan string)

	for _, input := range inputs {
		wg.Add(1)

		go func(input string) {
			defer wg.Done()

			for _, word := range mapWords(input) {
				wordCh <- word
			}
		}(input)
	}

	// Close the channel once all map goroutines are done.
	go func() {
		wg.Wait()
		close(wordCh)
	}()

	// Collect every word emitted by the maps.
	var words []string
	for word := range wordCh {
		words = append(words, word)
	}

	fmt.Println("there are the words ", words)
	return words
}

func main() {
	// The input is split into several pieces so the maps can run in parallel.
	inputs := []string{
		"cat dog cat",
		"dog cat dog",
	}

	// Map: one goroutine per split.
	words := concurrentMap(inputs)
	fmt.Println("Map:", words)

	// Shuffle: group the words by key into buckets of 1s.
	buckets := shuffle(words)
	fmt.Println("Shuffle:", buckets)

	// Reduce: sum each bucket to obtain the word counts.
	result := reduce(buckets)
	fmt.Println("Reduce:", result)
}
