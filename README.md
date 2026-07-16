# shardedmap

## What is ShardedMap?

`shardedmap` is a high-performance, concurrent-safe map designed to eliminate lock contention in heavy read/write workloads.

### The Problem
A standard Go map wrapped in a single `sync.Mutex` forces every goroutine to wait in a single line. Under heavy concurrent load, this creates a massive performance bottleneck.

### The Solution: Sharding
Instead of using one giant map with one lock, `shardedmap` splits your data across multiple independent sub-maps (called **shards**), each with its own local lock.

```text
              [ Key Hash ]
                   │
     ┌─────────────┼─────────────┐
     ▼             ▼             ▼
[ Shard 1 ]   [ Shard 2 ]   [ Shard N ]
[  Lock 1 ]   [  Lock 2 ]   [  Lock N ]
```

## Installation

```bash
go get github.com/ekazakas/shardedmap@latest
```

Or pin a specific release:

```bash
go get github.com/ekazakas/shardedmap@v1.0.0
```

## Usage

```go
package main

import (
	"fmt"

	"github.com/ekazakas/shardedmap"
)

func main() {
	sm := shardedmap.New[string, int]()

	sm.Set("apples", 3)
	sm.Set("oranges", 5)

	v, ok := sm.Get("apples")
	fmt.Println(v, ok)

	for value := range sm.GetAll() {
		fmt.Println(value)
	}
}
```

## Concurrency model

`Map` is safe for concurrent use by multiple goroutines.

Implementation notes:

- writes are synchronized per shard
- reads use shard-level read locks
- iteration uses snapshots of individual shards
- `GetAll()` provides a weakly consistent view during concurrent mutation
- `Len()` is computed across shards and should be treated as approximate under heavy concurrent writes

## Versioning and releases

This project uses Semantic Versioning tags and GitHub Releases.

- `main` is the primary development branch
- releases are cut from Git tags like `v1.0.0`

Examples:

```bash
go get github.com/ekazakas/shardedmap@v1.0.0
go get github.com/ekazakas/shardedmap@latest
```

## Benchmarks

Write heavy workloads:
```text
goos: linux
goarch: amd64
pkg: github.com/ekazakas/shardedmap
cpu: Intel(R) Core(TM) Ultra 7 155H
BenchmarkWriteOnly
BenchmarkWriteOnly/ShardedMap_16_shards
BenchmarkWriteOnly/ShardedMap_16_shards-22         	19462414	        62.49 ns/op
BenchmarkWriteOnly/ShardedMap_32_shards
BenchmarkWriteOnly/ShardedMap_32_shards-22         	30187658	        42.89 ns/op
BenchmarkWriteOnly/ShardedMap_64_shards
BenchmarkWriteOnly/ShardedMap_64_shards-22         	40146799	        36.91 ns/op
BenchmarkWriteOnly/ShardedMap_128_shards
BenchmarkWriteOnly/ShardedMap_128_shards-22        	47181008	        27.67 ns/op
BenchmarkWriteOnly/ShardedMap_256_shards
BenchmarkWriteOnly/ShardedMap_256_shards-22        	61918213	        21.45 ns/op
BenchmarkWriteOnly/ShardedMap_512_shards
BenchmarkWriteOnly/ShardedMap_512_shards-22        	66880017	        19.62 ns/op
BenchmarkWriteOnly/ShardedMap_1024_shards
BenchmarkWriteOnly/ShardedMap_1024_shards-22       	81759836	        18.44 ns/op
BenchmarkWriteOnly/sync.Map_Baseline
BenchmarkWriteOnly/sync.Map_Baseline-22            	33074616	        34.98 ns/op
BenchmarkWriteOnly/MutexMap_Baseline
BenchmarkWriteOnly/MutexMap_Baseline-22            	 6566796	        157.8 ns/op
PASS
```

Read heavy workloads:
```text
goos: linux
goarch: amd64
pkg: github.com/ekazakas/shardedmap
cpu: Intel(R) Core(TM) Ultra 7 155H
BenchmarkReadHeavy
BenchmarkReadHeavy/ShardedMap_1024_shards
BenchmarkReadHeavy/ShardedMap_1024_shards-22         	95417835	        11.69 ns/op
BenchmarkReadHeavy/sync.Map_Baseline
BenchmarkReadHeavy/sync.Map_Baseline-22              	211990609	        6.039 ns/op
BenchmarkReadHeavy/MutexMap_Baseline
BenchmarkReadHeavy/MutexMap_Baseline-22              	40393324	        26.65 ns/op
PASS
```
