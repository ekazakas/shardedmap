# shardedmap

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
