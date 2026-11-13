package raft

import (
	"math/rand"
	"testing"
)

func BenchmarkMaxN(b *testing.B) {
	val := make([]int, 0, 1024*128)
	for i := 0; i < 1024*128; i++ {
		val = append(val, rand.Intn(1024*1024*30))
	}
	b.Run("Sort", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			v := maxNForSort(val, 10)
			_ = v
		}
	})
	b.Run("NoSort", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			v := maxN(val, 10)
			_ = v
		}
	})
}
