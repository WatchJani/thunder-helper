package main

import (
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
	b.BPTree[string, string]
	sync.Mutex
}

func NewStore() *Store {

	return &Store{
		BPTree: *b.NewBPTree[string, string](40_000, 50),
		Mutex:  sync.Mutex{},
		// Stream: NewStream(4000, 4096),
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

		for index < len(data) && key.GetKey() > string(data[index:index+15]) {
			index += 4000
		}

		//ima neka grska
		if index+4000 < len(data) {
			index += 4000 - SmallestThenKey(data[index:index+4000], key.GetKey())
		} else {
			index += len(data) - index
		}

		wg.Add(1)
		go s.Process(data[start:index], fileName.GetValue(), &wg)
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

func (s *Store) Process(data []byte, fileName string, wg *sync.WaitGroup) {
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

	if string(buf[:15]) > string(data[:15]) {
		if err := os.Remove(fileName); err != nil {
			log.Println(err)
		}
		//Remove key from tree
	}

	s.MergeSort(buf[:n], data)

	wg.Done()
}

// problem sa n, nije pun file :D
func (s *Store) MergeSort(file, buf []byte) {
	var fileP, bufP, freeP int

	free := make([]byte, 4000+len(buf))

	for fileP < len(file) && bufP < len(buf) {
		if string(file[fileP:fileP+15]) < string(buf[bufP:bufP+15]) {
			copy(free[freeP:], file[fileP:fileP+100])
			fileP += 100
		} else {
			copy(free[freeP:], buf[bufP:bufP+100])
			bufP += 100
		}

		freeP += 100
	}

	var pointer, position = &buf, bufP
	if fileP < len(file) {
		pointer, position = &file, fileP
	}

	copy(free[freeP:], (*pointer)[position:])

	freeP = 0

	for freeP+8000 <= len(free) || len(free)-freeP == 4000 {
		go s.WriteFile(free[freeP : freeP+4000])
		freeP += 4000
	}
}

func (s *Store) WriteFile(date []byte) {
	fileName := string(date[:15])

	file, err := os.Create("./save/" + fileName + ".bin")
	if err != nil {
		log.Println(err)
	}

	go file.Write(date)
}
