package main

import (
	"log"
	"os"
	"testing"
)

func BenchmarkCutter(b *testing.B) {
	b.StopTimer()
	tree := NewStore()

	tree.LoadKey()

	//simulation file
	buff, err := os.ReadFile("./key/random.bin")
	if err != nil {
		log.Println(err)
	}
	
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		tree.Cutter(buff)
	}
}
