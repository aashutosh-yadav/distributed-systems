// Package main simulates a MapReduce cluster on a single machine by using
// goroutines as "virtual nodes" (worker machines) and channels as the network
// that connects them.
//
// This mirrors the design of mapreduce.go but replaces the single sequential
// pass with a coordinator and a pool of worker goroutines:
//
//	coordinator ──map tasks──► worker goroutines ──map results──► coordinator
//	coordinator ──shuffle──► grouped buckets
//	coordinator ──reduce tasks──► worker goroutines ──reduce results──► coordinator
//
// Because the workers run concurrently and communicate only through channels,
// the shape matches a real MapReduce cluster. The difference is that all the
// "machines" share this process's memory instead of talking over a network,
// so it is a simulation, not a true distributed deployment.
//
// Run it with:
//
//	go run cluster/mapreduce_cluster.go
package main

import (
	"fmt"
	"sort"
	"strings"
	"sync"
)

// KV is one key/value record. Map functions emit KV pairs, and the final
// reduce output is also expressed as KV pairs.
type KV struct {
	Key   string
	Value string
}

// mapFunc is the user-supplied Map function.
//
// It receives one input document and emits an intermediate key/value pair for
// each word it contains, with a value of "1". In a real cluster this function
// would run independently on each worker for its assigned split of the input.
func mapFunc(doc string) []KV {
	words := strings.Fields(doc)
	kvs := make([]KV, 0, len(words))

	for _, word := range words {
		kvs = append(kvs, KV{Key: word, Value: "1"})
	}

	return kvs
}

// reduceFunc is the user-supplied Reduce function.
//
// It receives one key together with every value that was emitted for that key
// and returns a single result. Here it simply counts the values, producing the
// word count for that key.
func reduceFunc(key string, values []string) string {
	return fmt.Sprintf("%d", len(values))
}

// taskKind distinguishes the two kinds of work a worker can be handed.
type taskKind int

const (
	kindMap taskKind = iota
	kindReduce
)

// task is a unit of work sent from the coordinator to a worker.
//
// A single channel carries both Map and Reduce tasks, so the same pool of
// workers performs both phases — exactly as a real cluster reuses its worker
// machines across the map and reduce stages.
type task struct {
	kind   taskKind
	id     int
	doc    string   // set for Map tasks
	key    string   // set for Reduce tasks
	values []string // set for Reduce tasks
}

// taskResult is a worker's reply sent back to the coordinator.
type taskResult struct {
	kind  taskKind
	id    int
	kvs   []KV   // set for Map results
	key   string // set for Reduce results
	value string // set for Reduce results
}

// worker is a virtual node.
//
// It continuously receives tasks from the coordinator, runs the appropriate
// Map or Reduce function, and sends the result back. It keeps working until
// the tasks channel is closed, which happens once every phase has finished.
func worker(id int, tasks <-chan task, results chan<- taskResult, wg *sync.WaitGroup) {
	defer wg.Done()

	for t := range tasks {
		switch t.kind {
		case kindMap:
			results <- taskResult{kind: kindMap, id: t.id, kvs: mapFunc(t.doc)}

		case kindReduce:
			value := reduceFunc(t.key, t.values)
			results <- taskResult{kind: kindReduce, id: t.id, key: t.key, value: value}
		}
	}
}

// runCluster is the coordinator.
//
// It splits the input into per-document Map tasks, collects the intermediate
// key/value pairs, performs the shuffle (grouping values by key), then hands
// out Reduce tasks and collects the final output.
//
//	nWorkers – how many virtual nodes (goroutines) to run.
//
// It returns the finished key/value results sorted by key, so the output is
// deterministic even though the workers run concurrently.
func runCluster(docs []string, nWorkers int) []KV {
	tasks := make(chan task)
	results := make(chan taskResult)

	// Start the pool of worker goroutines. Every worker can handle any task.
	var wg sync.WaitGroup
	for i := 1; i <= nWorkers; i++ {
		wg.Add(1)
		go worker(i, tasks, results, &wg)
	}

	// --- Map phase ---
	// Feed one Map task per input document to the workers.
	go func() {
		for i, doc := range docs {
			tasks <- task{kind: kindMap, id: i, doc: doc}
		}
	}()

	// Collect exactly one Map result per document and gather all of the
	// intermediate key/value pairs they produced.
	intermediate := make([]KV, 0)
	for i := 0; i < len(docs); i++ {
		res := <-results
		intermediate = append(intermediate, res.kvs...)
	}

	// --- Shuffle phase ---
	// Group every value by its key. This is the step a real cluster performs
	// over the network, routing each intermediate record to the reducer that
	// owns its key.
	buckets := make(map[string][]string)
	for _, kv := range intermediate {
		buckets[kv.Key] = append(buckets[kv.Key], kv.Value)
	}

	// Sort the keys so the Reduce tasks (and final output) are deterministic.
	keys := make([]string, 0, len(buckets))
	for key := range buckets {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	// --- Reduce phase ---
	// Feed one Reduce task per key to the same worker pool.
	go func() {
		for i, key := range keys {
			tasks <- task{kind: kindReduce, id: i, key: key, values: buckets[key]}
		}
	}()

	// Collect exactly one Reduce result per key.
	final := make([]KV, 0, len(keys))
	for range keys {
		res := <-results
		final = append(final, KV{Key: res.key, Value: res.value})
	}

	// All work is dispatched; tell the workers to stop and wait for them.
	close(tasks)
	wg.Wait()

	sort.Slice(final, func(i, j int) bool { return final[i].Key < final[j].Key })
	return final
}

// main defines the sample input and number of worker nodes, runs the
// simulated cluster, and prints the result.
func main() {
	docs := []string{
		"cat dog cat dog cat",
		"dog bird cat",
		"bird bird fish",
	}
	const nWorkers = 3

	fmt.Println("Input documents:")
	for i, doc := range docs {
		fmt.Printf("  doc %d: %q\n", i, doc)
	}
	fmt.Printf("\nRunning %d worker nodes...\n\n", nWorkers)

	result := runCluster(docs, nWorkers)

	fmt.Println("Final word counts:")
	for _, kv := range result {
		fmt.Printf("  %-6s %s\n", kv.Key, kv.Value)
	}
}
