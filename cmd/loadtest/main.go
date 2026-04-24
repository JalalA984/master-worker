// Load tester for the master-worker orchestrator.
// Submits inline tasks at a configurable rate and tracks latency + throughput.
//
// Usage: go run cmd/loadtest/main.go [-rate 10] [-count 50] [-addr http://localhost:9092]
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	addr := flag.String("addr", "http://localhost:9092", "Master HTTP address")
	ratePerSec := flag.Int("rate", 10, "Tasks per second")
	count := flag.Int("count", 50, "Total tasks to submit")
	flag.Parse()

	fmt.Println("=== Master-Worker Load Test ===")
	fmt.Printf("Target:  %s\n", *addr)
	fmt.Printf("Rate:    %d tasks/sec\n", *ratePerSec)
	fmt.Printf("Count:   %d tasks\n\n", *count)

	var (
		mu        sync.Mutex
		latencies []time.Duration
		errors    atomic.Int32
		submitted atomic.Int32
	)

	interval := time.Second / time.Duration(*ratePerSec)
	start := time.Now()
	var wg sync.WaitGroup

	for i := range *count {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()

			body, _ := json.Marshal(map[string]string{
				"language": "bash",
				"script":   fmt.Sprintf("echo 'load test task %d'; sleep 1", n),
			})

			reqStart := time.Now()
			resp, err := http.Post(*addr+"/api/v1/submit", "application/json", bytes.NewReader(body))
			latency := time.Since(reqStart)

			if err != nil {
				errors.Add(1)
				return
			}
			resp.Body.Close()
			if resp.StatusCode != 200 {
				errors.Add(1)
				return
			}

			submitted.Add(1)
			mu.Lock()
			latencies = append(latencies, latency)
			mu.Unlock()
		}(i)
		time.Sleep(interval)
	}

	wg.Wait()
	elapsed := time.Since(start)

	// Compute percentiles.
	mu.Lock()
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	mu.Unlock()

	p := func(pct float64) time.Duration {
		if len(latencies) == 0 {
			return 0
		}
		idx := int(float64(len(latencies)) * pct)
		if idx >= len(latencies) {
			idx = len(latencies) - 1
		}
		return latencies[idx]
	}

	fmt.Println("\n=== Results ===")
	fmt.Printf("Duration:    %v\n", elapsed.Round(time.Millisecond))
	fmt.Printf("Submitted:   %d\n", submitted.Load())
	fmt.Printf("Errors:      %d\n", errors.Load())
	fmt.Printf("Throughput:  %.1f tasks/sec\n", float64(submitted.Load())/elapsed.Seconds())
	fmt.Printf("Latency p50: %v\n", p(0.50))
	fmt.Printf("Latency p95: %v\n", p(0.95))
	fmt.Printf("Latency p99: %v\n", p(0.99))
}
