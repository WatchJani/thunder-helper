package gen

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"slices"
)

const (
	KeySize       = 15
	MaxDataLength = 100 // byte
	PathStore     = "./example/"
	ExtensionBin  = ".bin"
	NumberOfFiles = 2048

	SizeOfFile = 4 * 1024
	DataSize   = 100
)

func Generate() {
	maxNumberOfData := NumberOfFiles * (SizeOfFile / DataSize)

	mySimpleIDGenerator := make([]string, maxNumberOfData)

	for index := range mySimpleIDGenerator {
		mySimpleIDGenerator[index] = GeneratorRandom(KeySize, '0', '9')
	}

	slices.Sort(mySimpleIDGenerator)

	dataGenerated := GeneratorFile(mySimpleIDGenerator)

	maxSizePerFile := SizeOfFile / DataSize * 100

	keyBuffer := make([]byte, NumberOfFiles*KeySize)
	var keyCounter int

	for index := 0; index < len(dataGenerated); index += maxSizePerFile {
		copy(keyBuffer[keyCounter:], dataGenerated[index:index+15])
		keyCounter += 15
		SaveFile(dataGenerated[index : index+maxSizePerFile])
	}

	os.WriteFile("./key/key.bin", keyBuffer, 0766)
}

func GeneratorFile(IDs []string) []byte {
	buf := make([]byte, NumberOfFiles*4*1000)

	for index, pointer := 0, 0; pointer < len(buf); pointer, index = pointer+MaxDataLength, index+1 {
		copy(buf[pointer:], []byte(IDs[index]+"|"+GeneratorRandom(MaxDataLength-KeySize-1, 'a', 'z')))
	}

	return buf
}

// func Format(ID string) []byte {
// 	return []byte(fmt.Sprintf("%s|%s\n", ID, GeneratorRandom(MaxDataLength-KeySize-2, 'a', 'z')))
// }

func SaveFile(data []byte) {
	if err := os.WriteFile(Path(fmt.Sprintf("%s_%s", data[:KeySize], data[len(data)-MaxDataLength:len(data)-MaxDataLength+KeySize]), ExtensionBin), data, 0766); err != nil {
		log.Println(err)
	}
}

func Path(name, extension string) string {
	return PathStore + name + extension
}

func GeneratorRandom(size, min, max int) string {
	ID := make([]byte, size)

	for index := range ID {
		ID[index] = byte(rand.Intn(max-min) + min)
	}

	return string(ID)
}

func Generate8MB() {
	maxNumberOfData := 1 * (8 * 1024 * 1024 / 100)

	mySimpleIDGenerator := make([]string, maxNumberOfData)

	buffer := make([]byte, 8*1024*1024)

	for index := range mySimpleIDGenerator {
		mySimpleIDGenerator[index] = GeneratorRandom(15, '0', '9')
	}

	slices.Sort(mySimpleIDGenerator)

	for index, key := range mySimpleIDGenerator {
		copy(buffer[index*100:], []byte(fmt.Sprintf("%s%s", key, GeneratorRandom(85, 'a', 'z'))))
	}

	os.WriteFile("./key/random.bin", buffer, 0766)
}

func ReadAllFileName(dirPath string) []string {
	filesName := make([]string, 0, 2048)

	files, err := os.ReadDir(dirPath)
	if err != nil {
		log.Fatal(err)
	}

	for _, file := range files {
		filesName = append(filesName, file.Name())
	}

	return filesName
}
