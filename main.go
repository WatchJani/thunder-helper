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
	StackByte
}

func NewStore() *Store {
	return &Store{
		BPTree:    *b.NewBPTree[string, string](40_000, 50),
		Mutex:     sync.Mutex{},
		StackByte: NewStackByte(1024, 8*1024),
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

	for {

	}
}

type StackByte struct {
	freeSpace [][]byte
	counter   int
}

func NewStackByte(size, length int) StackByte {
	space := make([][]byte, size)

	for index := range space {
		space[index] = make([]byte, length)
	}

	return StackByte{
		freeSpace: space,
		counter:   size,
	}
}

func (s *StackByte) GetOne() int {
	s.counter--
	return s.counter + 1
}

func (s *Store) Cutter(data []byte) {
	index, wg := 0, sync.WaitGroup{}
	for index < len(data) {
		start := index

		s.PositionSearch(string(data[index : index+15]))

		fileName, err := s.GetCurrentKey()
		if err != nil {
			// log.Println(err)
			s.NextKey()
		}

		s.NextKey()

		key, err := s.GetCurrentKey()
		if err != nil {
			log.Println(err)
		}

		for index < len(data) && key.GetKey() > string(data[index+3900:index+3915]) {
			index += 4000
		}

		if len(key.GetKey()) > 0 {
			if index+4000 < len(data) {
				index += SmallestThenKey(data[index:index+4000], key.GetKey())
			} else {
				index += SmallestThenKey(data[index:], key.GetKey())
			}
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
			return index + 100
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

func (s *Store) MergeSort(file, buf []byte) {
	var fileP, bufP, freeP int

	free := make([]byte, len(file)+len(buf))

	for fileP < len(file) && bufP < len(buf) {
		if string(file[fileP:fileP+15]) < string(buf[bufP:bufP+15]) {
			copy(free[freeP:], file[fileP:fileP+100])
			fileP += 100
		} else if string(file[fileP:fileP+15]) > string(buf[bufP:bufP+15]) {
			copy(free[freeP:], buf[bufP:bufP+100])
			bufP += 100
		} else { //is key the equal then replace //update
			free = free[:len(free)-100]
			copy(free[freeP:], buf[bufP:bufP+100])
			fileP, bufP = fileP+100, bufP+100
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
		s.WriteFile(free[freeP : freeP+4000])
		freeP += 4000
	}

	if freeP < len(free) {
		half := RoundUp(freeP+(len(free)-freeP)/2, 100)
		s.WriteFile(free[freeP:half])
		s.WriteFile(free[half:])
	}
}

func RoundUp(index, dataLength int) int {
	return index - (index % dataLength)
}

func (s *Store) WriteFile(data []byte) {
	var wg sync.WaitGroup
	wg.Add(1)
	go WriteFile(data, &wg)
	wg.Wait()

	// s.Mutex.Lock()
	// s.Insert(string(data[:15]), string(data[:15])+".bin")
	// s.Mutex.Unlock()
}

func WriteFile(data []byte, wg *sync.WaitGroup) {
	fileName := string(data[:15])

	file, err := os.OpenFile("./save/"+fileName+".bin", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Println(err)
		return
	}

	defer file.Close()

	if _, err := file.Write(data); err != nil {
		log.Println(err)
		return
	}

	wg.Done()
}

// later implement:D
func FolderGroup(fileName string) string {
	path := make([]byte, 19)
	for index, pathI := 0, 0; index < len(fileName); index += 5 {
		copy(path[pathI:], []byte(fileName[index:index+5]))
		pathI += 5
		path[pathI] = '/'
		pathI++
	}

	return string(path[:len(path)-1])
}
