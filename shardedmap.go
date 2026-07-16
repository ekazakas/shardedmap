// Package shardedmap provides a highly concurrent, thread-safe sharded map
// implementation using Go generics and modern iterator patterns.
//
// By partitioning the keyspace into multiple independent shards, lock contention
// is drastically reduced, enabling high-throughput write and read operations
// across multiple goroutines.
package shardedmap

import (
	"hash/maphash"
	"iter"
	"sync"
)

const (
	// DefaultShardCount is the default number of shards used when initializing
	// a new Map without an explicit Config.
	DefaultShardCount = 32
)

type (
	// Config holds configuration parameters for initializing a new [Map].
	Config struct {
		// ShardCount specifies the number of internal partitions to use.
		// It must be a positive power of 2 (e.g., 16, 32, 64, 128).
		// If set to 0, [DefaultShardCount] will be used.
		ShardCount int
	}

	// Map is a high-performance, concurrent, thread-safe map.
	// It distributes keys across multiple independent shards to minimize lock contention.
	//
	// K represents the comparable key type, and T represents the value type.
	//
	// It is safe for concurrent use by multiple goroutines.
	Map[K comparable, T any] struct {
		mask   uint64
		shards []*shard[K, T]
		seed   maphash.Seed
	}

	// shard is an individual, mutex-protected partition within the [Map].
	shard[K comparable, T any] struct {
		items map[K]T
		mu    sync.RWMutex
	}
)

// New initializes and returns a new [Map] configured with the sane defaults.
func New[K comparable, T any]() *Map[K, T] {
	return NewWithConfig[K, T](Config{
		ShardCount: DefaultShardCount,
	})
}

// NewWithConfig initializes and returns a new [Map] using the provided [Config].
//
// It will panic if:
//   - ShardCount is less than 0
//   - ShardCount is not a power of 2
//
// If ShardCount is 0, it falls back to [DefaultShardCount].
func NewWithConfig[K comparable, T any](cfg Config) *Map[K, T] {
	if cfg.ShardCount == 0 {
		cfg.ShardCount = DefaultShardCount
	}

	if cfg.ShardCount <= 0 {
		panic(`shard count must be greater than 0`)
	}

	if (cfg.ShardCount & (cfg.ShardCount - 1)) != 0 {
		panic(`shard count must be a power of 2`)
	}

	shards := make([]*shard[K, T], cfg.ShardCount)
	for i := range cfg.ShardCount {
		shards[i] = &shard[K, T]{
			items: make(map[K]T),
		}
	}

	return &Map[K, T]{
		mask:   uint64(cfg.ShardCount - 1),
		shards: shards,
		seed:   maphash.MakeSeed(),
	}
}

// Set associates the specified value with the given key in the map.
// If the key already exists, its value is updated.
func (sm *Map[K, T]) Set(key K, value T) {
	sm.getShard(key).Set(key, value)
}

// SetAll writes all key-value pairs from the provided Go map into the sharded map.
// This operation is not atomic across shards; other goroutines may see partial writes.
func (sm *Map[K, T]) SetAll(data map[K]T) {
	for k, v := range data {
		sm.getShard(k).Set(k, v)
	}
}

// SetIfAbsent associates the specified value with the given key only if the key
// does not already exist in the map.
//
// It returns true if the key was absent and the value was set, or false if the key
// was already present and no write occurred.
func (sm *Map[K, T]) SetIfAbsent(key K, value T) bool {
	return sm.getShard(key).SetIfAbsent(key, value)
}

// Remove deletes the value associated with the given key from the map.
//
// It returns the removed value and a boolean indicating whether the key was found.
// If the key was not found, it returns the zero value of T and false.
func (sm *Map[K, T]) Remove(key K) (T, bool) {
	return sm.getShard(key).Remove(key)
}

// Get retrieves the value associated with the given key.
//
// It returns the value and a boolean indicating whether the key exists in the map.
// If the key is not found, it returns the zero value of T and false.
func (sm *Map[K, T]) Get(key K) (T, bool) {
	return sm.getShard(key).Get(key)
}

// Clear removes all elements from all internal shards in the map.
func (sm *Map[K, T]) Clear() {
	for _, s := range sm.shards {
		s.Clear()
	}
}

// Len returns the total number of elements currently stored across all shards in the map.
//
// Note that this is calculated by taking read locks on each shard sequentially.
// Under highly concurrent writes, the returned value is a close approximation of the size.
func (sm *Map[K, T]) Len() int {
	l := 0

	for _, s := range sm.shards {
		l += s.Len()
	}

	return l
}

// IsEmpty returns true if there are no elements stored in the map.
func (sm *Map[K, T]) IsEmpty() bool {
	return sm.Len() == 0
}

// GetAll returns a modern Go iterator ([iter.Seq]) yielding all values currently
// stored in the map.
//
// Because it iterates through shards sequentially, it provides a weakly consistent view
// (or "read-committed" view) of the map. This means:
//   - It is safe to concurrently read and mutate the map while iterating.
//   - Iteration will never deadlock or block other goroutines long-term.
//   - Any mutations made during iteration may or may not be visible to the iterator.
func (sm *Map[K, T]) GetAll() iter.Seq[T] {
	return func(yield func(T) bool) {
		for _, s := range sm.shards {
			if !s.StreamShard(yield) {
				return
			}
		}
	}
}

// getShard hashes the key and uses bitwise masking to locate the appropriate partition.
func (sm *Map[K, T]) getShard(key K) *shard[K, T] {
	return sm.shards[maphash.Comparable(sm.seed, key)&sm.mask]
}

// Set locks the shard and writes the key-value pair.
func (s *shard[K, T]) Set(key K, value T) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.items[key] = value
}

// SetIfAbsent locks the shard and writes the value only if the key is missing.
func (s *shard[K, T]) SetIfAbsent(key K, value T) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, ok := s.items[key]
	if !ok {
		s.items[key] = value
	}

	return !ok
}

// Remove locks the shard and deletes the key, returning the value and presence.
func (s *shard[K, T]) Remove(key K) (T, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	node, ok := s.items[key]
	if !ok {
		var zero T
		return zero, false
	}

	delete(s.items, key)

	return node, true
}

// Clear locks the shard and re-allocates its internal map.
func (s *shard[K, T]) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.items = make(map[K]T)
}

// Get RLocks the shard to perform a concurrent-safe read.
func (s *shard[K, T]) Get(key K) (T, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	t, ok := s.items[key]

	return t, ok
}

// Len RLocks the shard to retrieve the element count.
func (s *shard[K, T]) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return len(s.items)
}

// StreamShard takes a local snapshot of the shard and streams it to the iterator yield.
func (s *shard[K, T]) StreamShard(yield func(T) bool) bool {
	snapshot := s.Snapshot()

	for _, t := range snapshot {
		if !yield(t) {
			return false
		}
	}

	return true
}

// Snapshot takes a brief RLock to clone the shard's values into a slice.
// This decouples long-running iterations from the shard's read/write lock.
func (s *shard[K, T]) Snapshot() []T {
	s.mu.RLock()
	defer s.mu.RUnlock()

	snapshot := make([]T, 0, len(s.items))
	for _, t := range s.items {
		snapshot = append(snapshot, t)
	}

	return snapshot
}
