package shardedmap_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/ekazakas/shardedmap"
	"github.com/stretchr/testify/require"
)

type (
	mutexMap[K comparable, T any] struct {
		mu sync.RWMutex
		m  map[K]T
	}
)

func newMutexMap[K comparable, T any]() *mutexMap[K, T] {
	return &mutexMap[K, T]{m: make(map[K]T)}
}

func (mm *mutexMap[K, T]) Get(key K) (T, bool) {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	v, ok := mm.m[key]
	return v, ok
}

func (mm *mutexMap[K, T]) Set(key K, value T) {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	mm.m[key] = value
}

func TestShardedMap_InitializationAndPanics(t *testing.T) {
	smDefault := shardedmap.New[string, int]()
	require.True(t, smDefault.IsEmpty())

	smZeroConfig := shardedmap.NewWithConfig[string, int](shardedmap.Config{
		ShardCount: 0,
	})
	require.NotNil(t, smZeroConfig)

	require.Panics(t, func() {
		shardedmap.NewWithConfig[string, int](shardedmap.Config{
			ShardCount: -5,
		})
	}, "Should panic on negative shard count")

	require.Panics(t, func() {
		shardedmap.NewWithConfig[string, int](shardedmap.Config{
			ShardCount: 7,
		})
	}, "Should panic when shard count is not a power of 2")
}

func TestShardedMap_BasicOperations(t *testing.T) {
	sm := shardedmap.New[int, string]()
	require.True(t, sm.IsEmpty(), "New map should be empty")
	require.Equal(t, 0, sm.Len(), "New map should have length 0")

	inserted := sm.SetIfAbsent(1, "v1")
	require.True(t, inserted, "Should successfully insert an absent key")
	require.Equal(t, 1, sm.Len())
	require.False(t, sm.IsEmpty())

	insertedAgain := sm.SetIfAbsent(1, "clash")
	require.False(t, insertedAgain, "Should return false when key already exists")

	sm.Set(2, "v2")
	val, found := sm.Get(1)
	require.True(t, found, "Key 1 should be found")
	require.Equal(t, "v1", val, "Value should not have been overwritten by 'clash'")

	batch := map[int]string{
		3: "v3",
		4: "v4",
	}
	sm.SetAll(batch)
	require.Equal(t, 4, sm.Len(), "Map length should match total items added")

	removed, ok := sm.Remove(1)
	require.True(t, ok, "Key 1 should be successfully removed")
	require.Equal(t, "v1", removed)

	missingVal, foundMissing := sm.Remove(999)
	require.False(t, foundMissing, "Removing a non-existent key should return false")
	require.Empty(t, missingVal, "Removing a non-existent key should return the zero value")

	_, found = sm.Get(1)
	require.False(t, found, "Key 1 should no longer exist")
	require.Equal(t, 3, sm.Len())

	sm.Clear()
	require.True(t, sm.IsEmpty(), "Map should be empty after Clear")
	require.Equal(t, 0, sm.Len())

	_, found = sm.Get(2)
	require.False(t, found, "Key 2 should no longer exist after Clear")
}

func TestShardedMap_GetAll_And_DeadlockImmunity(t *testing.T) {
	sm := shardedmap.NewWithConfig[string, string](shardedmap.Config{
		ShardCount: 16,
	})
	sm.Set("k1", "v1")
	sm.Set("k2", "v2")
	sm.Set("k3", "v3")

	count := 0
	for range sm.GetAll() {
		count++
		sm.Set(fmt.Sprintf("new_key_%d", count), "new_val")
	}

	require.GreaterOrEqual(t, count, 3, "Should see at least the 3 initial elements")
}

func TestShardedMap_Concurrency(t *testing.T) {
	sm := shardedmap.New[int, int]()
	var wg sync.WaitGroup

	numGoroutines := 50
	operationsPerGoroutine := 100

	for i := range numGoroutines {
		wg.Go(func() {
			for j := range operationsPerGoroutine {
				key := i*operationsPerGoroutine + j
				sm.Set(key, j)
			}
		})
	}

	for range 5 {
		wg.Go(func() {
			for val := range sm.GetAll() {
				_ = val
			}
		})
	}

	wg.Wait()

	totalExpectedItems := numGoroutines * operationsPerGoroutine
	actualItems := 0
	for range sm.GetAll() {
		actualItems++
	}

	require.Equal(t, totalExpectedItems, actualItems)
}

func BenchmarkWriteOnly(b *testing.B) {
	shardCounts := []int{16, 32, 64, 128, 256, 512, 1024}

	for _, sc := range shardCounts {
		b.Run(fmt.Sprintf("ShardedMap_%d_shards", sc), func(b *testing.B) {
			sm := shardedmap.NewWithConfig[int, int](shardedmap.Config{ShardCount: sc})
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					sm.Set(i, i)
					i++
				}
			})
		})
	}

	b.Run("sync.Map_Baseline", func(b *testing.B) {
		var m sync.Map
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				m.Store(i, i)
				i++
			}
		})
	})

	b.Run("MutexMap_Baseline", func(b *testing.B) {
		mm := newMutexMap[int, int]()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				mm.Set(i, i)
				i++
			}
		})
	})
}

func BenchmarkReadHeavy(b *testing.B) {
	const hotKeys = 10_000

	b.Run("ShardedMap_1024_shards", func(b *testing.B) {
		sm := shardedmap.NewWithConfig[int, int](shardedmap.Config{ShardCount: 1024})
		for i := range hotKeys {
			sm.Set(i, i)
		}
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				if i%10 == 0 {
					sm.Set(i%hotKeys, i)
				} else {
					_, _ = sm.Get(i % hotKeys)
				}
				i++
			}
		})
	})

	b.Run("sync.Map_Baseline", func(b *testing.B) {
		var m sync.Map
		for i := range hotKeys {
			m.Store(i, i)
		}
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				if i%10 == 0 {
					m.Store(i%hotKeys, i)
				} else {
					_, _ = m.Load(i % hotKeys)
				}
				i++
			}
		})
	})

	b.Run("MutexMap_Baseline", func(b *testing.B) {
		mm := newMutexMap[int, int]()
		for i := range hotKeys {
			mm.Set(i, i)
		}
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				if i%10 == 0 {
					mm.Set(i%hotKeys, i)
				} else {
					_, _ = mm.Get(i % hotKeys)
				}
				i++
			}
		})
	})
}
