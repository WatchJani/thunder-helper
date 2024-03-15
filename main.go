package main

import (
	"fmt"
	"log"
	"os"
	"root/gen"
	"sync"

	b "github.com/WatchJani/new-b-plus-tree/b_plus_tree"
)

const (
	DataLength = 100
	KeyLength  = 15
	KB4        = 4 * 1024
	KB8        = 8 * 1024
)

type Store struct {
	*b.BPTree[string, string]
}

func NewStore() *Store {
	return &Store{
		BPTree: b.NewBPTree[string, string](40_000, 50),
	}
}

func (s *Store) LoadKey() {
	keyBuffer, err := os.ReadFile("./key/key.bin")
	if err != nil {
		log.Println(err)
	}

	filesName := gen.ReadAllFileName("./example")

	//add every key to BPTree
	for i, j := 0, 0; i < len(keyBuffer); i, j = i+15, j+1 {
		s.Insert(string(keyBuffer[i:i+15]), filesName[j])
	}

}

// test functionality
func main() {
	tree := NewStore()

	tree.LoadKey()

	// simulation file
	buff, err := os.ReadFile("./key/random.bin")
	if err != nil {
		log.Println(err)
	}

	tree.Cutter(buff)
}

func (s *Store) Cutter(data []byte) {
	index, wg := 0, sync.WaitGroup{}
	for index < len(data) {
		start := index

		s.PositionSearch(string(data[index : index+15]))

		fileName, err := s.GetCurrentKey()
		if err != nil {
			log.Println(err)
		}

		s.NextKey()

		key, err := s.GetCurrentKey()
		if err != nil {
			log.Println(err)
		}

		for index+3915 < len(data) && key.GetKey() > string(data[index+3900:index+3900+15]) {
			index += 4000
		}

		if index+4000 < len(data) {
			index += 4000 - SmallestThenKey(data[index:index+4000], key.GetKey())
		} else {
			index += len(data) - index
		}

		wg.Add(1)
		go Process(data[start:index], fileName.GetValue(), &wg)
	}

	wg.Wait()
	//delete 8mb file
}

func SmallestThenKey(data []byte, key string) int {
	for index := len(data) - 100; index >= 0; index -= 100 {
		if string(data[index:index+15]) < key {
			return index
		}
	}

	return 0
}

func Process(data []byte, fileName string, wg *sync.WaitGroup) {
	file, err := os.Open("./example/" + fileName)
	if err != nil {
		log.Println(err)
	}

	defer file.Close()

	buf := make([]byte, KB4) //add in system as global state

	n, err := file.Read(buf)
	if err != nil {
		log.Println(err)
	}

	fmt.Println()
	fmt.Println(string(data))
	fmt.Println()
	fmt.Println(string(buf[:n]))
	fmt.Printf("============================")

	free := make([]byte, KB4*len(data)/4000+4000) //add in system as global state

	var i, j, f int
	for i+15 < n && j+15 < len(data) {
		if string(buf[i:i+KeyLength]) < string(data[j:j+KeyLength]) {
			copy(free[:f], buf[i:i+DataLength])
			i += DataLength
		} else {
			copy(free[:f], data[j:j+KeyLength])
			j += DataLength
		}

		f += DataLength
	}

	wg.Done()
}

func MergeSort(file, buf []byte) {
	var fileP, bufP, freeP int

	free := make([]byte, 4096)

	for fileP < len(file) && bufP < len(buf) {
		if freeP+100 > len(free) {
			freeP = 0 //Reset
			//go Write File
			free = make([]byte, 4096) // get new buffer from the store
		}

		//need optimization for this part, just when we find bigger then copy
		if string(file[fileP:fileP+15]) < string(buf[bufP:bufP+15]) {
			copy(free[freeP:], file[fileP:fileP+100])
			fileP += 100
		} else {
			copy(free[freeP:], buf[bufP:bufP+100])
			bufP += 100
		}

		freeP += 100
	}

	//strange but magical
	var pointer, position = &buf, bufP
	if fileP < len(file) {
		pointer, position = &file, fileP
	}

	end := len(free) - fileP

	for position < len(*pointer) {
		if freeP+100 > len(free) {
			*pointer = make([]byte, 4096)
		}

		if position+end < len(*pointer) {
			copy(free[freeP:], (*pointer)[position:position+end])
			end, freeP = end+4000, freeP+position-end
		} else {
			copy(free[freeP:], (*pointer)[position:position+len((*pointer))-1-position])
			break
		}
	}

}
