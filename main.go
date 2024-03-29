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

	Format string = ".bin"
)

type Store struct {
	b.BPTree[string, string]
	sync.Mutex
	keyLength         int
	dataLength        int
	maxDataByteInFile int
}

func NewStore() *Store {
	return &Store{
		BPTree:            *b.NewBPTree[string, string](40_000, 50),
		Mutex:             sync.Mutex{},
		keyLength:         15,
		dataLength:        100,
		maxDataByteInFile: (4 * 1024) - (4*1024)%100,
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

func (s *Store) Cutter(data []byte) {
	index, wg := 0, sync.WaitGroup{}
	for index < len(data) {
		start := index

		s.PositionSearch(string(data[index : index+s.keyLength]))

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

		for index < len(data) && key.GetKey() > string(data[index+s.maxDataByteInFile-s.dataLength:index+s.maxDataByteInFile-s.dataLength+s.keyLength]) {
			index += s.maxDataByteInFile
		}

		if len(key.GetKey()) > 0 {
			if index+s.maxDataByteInFile < len(data) {
				index += s.SmallestThenKey(data[index:index+s.maxDataByteInFile], key.GetKey())
			} else {
				index += s.SmallestThenKey(data[index:], key.GetKey())
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

func (s Store) SmallestThenKey(data []byte, key string) int {
	for index := len(data) - s.dataLength; index >= 0; index -= s.dataLength {
		if string(data[index:index+s.keyLength]) < key {
			return index + s.dataLength
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

	if string(buf[:s.keyLength]) > string(data[:s.keyLength]) {
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
		if string(file[fileP:fileP+s.keyLength]) < string(buf[bufP:bufP+s.keyLength]) {
			copy(free[freeP:], file[fileP:fileP+s.dataLength])
			fileP += s.dataLength
		} else if string(file[fileP:fileP+s.keyLength]) > string(buf[bufP:bufP+s.keyLength]) {
			copy(free[freeP:], buf[bufP:bufP+s.dataLength])
			bufP += s.dataLength
		} else {
			free = free[:len(free)-s.dataLength]
			copy(free[freeP:], buf[bufP:bufP+s.dataLength])
			fileP, bufP = fileP+s.dataLength, bufP+s.dataLength
		}

		freeP += s.dataLength
	}

	var pointer, position = &buf, bufP
	if fileP < len(file) {
		pointer, position = &file, fileP
	}

	copy(free[freeP:], (*pointer)[position:])

	freeP = 0

	for freeP+2*s.maxDataByteInFile <= len(free) || len(free)-freeP == s.maxDataByteInFile {
		s.WriteFile(free[freeP : freeP+s.maxDataByteInFile])
		freeP += s.maxDataByteInFile
	}

	if freeP < len(free) {
		half := RoundUp(freeP+(len(free)-freeP)/2, s.dataLength)
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
	go s.Write(data, &wg)
	wg.Wait()

	// s.Mutex.Lock()
	// s.Insert(string(data[:15]), string(data[:15])+".bin")
	// s.Mutex.Unlock()
}

func (s Store) Write(data []byte, wg *sync.WaitGroup) {
	fileName := string(data[:s.keyLength])

	file, err := os.OpenFile("./save/"+fileName+Format, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
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
		path[pathI+5] = '/'
		pathI += 6
	}

	return string(path[:len(path)-2])
}
