package main

import "C"
import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"unsafe"
)

// #cgo CFLAGS: -I./
// #cgo LDFLAGS: -L./ -lerasurecode
// #include "erasure_coder.h"
// void load_erasurecode_lib(char* err, size_t err_len);
//
// typedef struct {
//     unsigned char *data[14];
// } EcShard;
//
// // Fix panic: runtime error: cgo argument has Go pointer to Go pointer
// void setPointer(unsigned char **pp, int index, unsigned char *p) {
//     pp[index] = p;
// }
import "C"

var dataShards = flag.Int("data", 4, "Number of shards to split the data into, must be below 257.")
var parShards = flag.Int("par", 2, "Number of parity shards")
var shardSize = flag.Int("shardSize", 2, "Size of shards in bytes")
var outDir = flag.String("out", "", "Alternative output directory")

func init() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  simple-encoder [-flags] filename.ext\n\n")
		fmt.Fprintf(os.Stderr, "Valid flags:\n")
		flag.PrintDefaults()
	}
}

func AllocAligned(shards, each int) [][]byte {
	eachAligned := ((each + 63) / 64) * 64
	total := make([]byte, eachAligned*shards+63)
	// We cannot do initial align without "unsafe", just use native alignment.
	res := make([][]byte, shards)
	for i := range res {
		res[i] = total[:each:eachAligned]
		total = total[eachAligned:]
	}
	return res
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func main() {
	// Parse command line parameters.
	flag.Parse()
	args := flag.Args()
	if len(args) != 1 {
		fmt.Fprintf(os.Stderr, "Error: No input filename given\n")
		flag.Usage()
		os.Exit(1)
	}
	if (*dataShards + *parShards) > 256 {
		fmt.Fprintf(os.Stderr, "Error: sum of data and parity shards cannot exceed 256\n")
		os.Exit(1)
	}
	fname := args[0]

	fmt.Println("Opening", fname)
	b, err := ioutil.ReadFile(fname)
	checkErr(err)

	shards2 := make([][]byte, *dataShards+*parShards)
	actualShardSize := min(len(b), *shardSize)
	i := 0
	for ; len(b) >= actualShardSize && i < *dataShards; i++ {
		shards2[i] = b[:actualShardSize:actualShardSize]
		b = b[actualShardSize:]
	}
	var padding [][]byte
	padding = AllocAligned(*dataShards-i+*parShards, actualShardSize)
	if i < *dataShards {
		if len(b) > 0 {
			copy(padding[0], b) // TODO: dont't need to fill zero?
		}
	}
	for j := i; j < *dataShards; j++ {
		shards2[j] = padding[0]
		padding = padding[1:]
	}
	for j := *dataShards; j < *dataShards+*parShards; j++ {
		shards2[j] = padding[0]
		padding = padding[1:]
	}

	// Write out the resulting files.
	dir, file := filepath.Split(fname)
	if *outDir != "" {
		dir = *outDir
	}

	// Use native erasure coding
	isalEncoder := C.IsalEncoder{}
	//size := (C.size_t)(C.sizeof_IsalEncoder)
	//pEncoder := (*C.IsalEncoder)(C.malloc(size))
	cErr := [256]byte{}
	C.load_erasurecode_lib((*C.char)(unsafe.Pointer(&cErr[0])), 256)
	C.initEncoder(&isalEncoder, (C.int)(*dataShards), (C.int)(*parShards))
	inputs := (**C.uchar)(C.malloc(C.size_t(*dataShards) * 8))
	for i = 0; i < *dataShards; i++ {
		C.setPointer(inputs, C.int(i), (*C.uchar)(unsafe.Pointer(&shards2[i][0])))
	}
	outputs := (**C.uchar)(C.malloc(C.size_t(*parShards) * 8))
	for i = 0; i < *parShards; i++ {
		C.setPointer(outputs, C.int(i), (*C.uchar)(unsafe.Pointer(&shards2[*dataShards+i][0])))
	}
	C.encode(&isalEncoder, inputs, outputs, C.int(actualShardSize))
	C.free(unsafe.Pointer(inputs))
	C.free(unsafe.Pointer(outputs))

	dir = *outDir + "-native"
	for i, shard := range shards2 {
		outfn := fmt.Sprintf("%s.%d", file, i)

		fmt.Println("Writing to", outfn)
		err = ioutil.WriteFile(filepath.Join(dir, outfn), shard, 0644)
		checkErr(err)
	}
}

func checkErr(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s", err.Error())
		os.Exit(2)
	}
}
