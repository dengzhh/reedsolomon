//go:build ignore
// +build ignore

// Copyright 2015, Klaus Post, see LICENSE for details.
//
// Simple encoder example
//
// The encoder encodes a simgle file into a number of shards
// To reverse the process see "simpledecoder.go"
//
// To build an executable use:
//
// go build simple-decoder.go
//
// Simple Encoder/Decoder Shortcomings:
// * If the file size of the input isn't divisible by the number of data shards
//   the output will contain extra zeroes
//
// * If the shard numbers isn't the same for the decoder as in the
//   encoder, invalid output will be generated.
//
// * If values have changed in a shard, it cannot be reconstructed.
//
// * If two shards have been swapped, reconstruction will always fail.
//   You need to supply the shards in the same order as they were given to you.
//
// The solution for this is to save a metadata file containing:
//
// * File size.
// * The number of data/parity shards.
// * HASH of each shard.
// * Order of the shards.
//
// If you save these properties, you should abe able to detect file corruption
// in a shard and be able to reconstruct your data if you have the needed number of shards left.

package main

import (
	"flag"
	"fmt"
	"github.com/klauspost/reedsolomon"
	"io/ioutil"
	"os"
	"path/filepath"
)

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

	// Create encoding matrix.
	enc, err := reedsolomon.New(*dataShards, *parShards, reedsolomon.WithAutoGoroutines(actualShardSize), reedsolomon.WithCauchyMatrix())
	// Not compatible with ozone EC. enc, err := reedsolomon.New(*dataShards, *parShards, reedsolomon.WithAutoGoroutines(actualShardSize))
	checkErr(err)

	// Encode parity
	err = enc.Encode(shards2)
	checkErr(err)

	// Write out the resulting files.
	dir, file := filepath.Split(fname)
	if *outDir != "" {
		dir = *outDir
	}
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
