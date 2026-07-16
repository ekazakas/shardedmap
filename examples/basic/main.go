package main

import (
	"fmt"

	shardedmap "github.com/ekazakas/shardedmap"
)

func main() {
	m := shardedmap.NewWithConfig[string, int](shardedmap.Config{
		ShardCount: 32,
	})

	m.Set("key1", 10)
	m.Set("key2", 20)

	if v, ok := m.Get("key2"); ok {
		fmt.Println("key2:", v)
	}

	if v, ok := m.Remove("key1"); ok {
		fmt.Println("key1:", v)
	}

	fmt.Println("len:", m.Len())

	for v := range m.GetAll() {
		fmt.Println(v)
	}

	m.Clear()

	fmt.Println("len:", m.Len())
	fmt.Println("empty:", m.IsEmpty())
}
