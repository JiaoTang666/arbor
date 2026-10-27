// Offline adapter only. Partitioning is performed by unchanged original CLPA.
package main

import (
	"blockEmulator/params"
	"blockEmulator/partition"
	"bufio"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"os"
	"time"
)

func main() {
	input := flag.String("input", "", "TRAIN graph binary")
	output := flag.String("output", "", "exclusive output int32 routes")
	shards := flag.Int("shards", 16, "shard count")
	flag.Parse()
	if *input == "" || *output == "" || *shards < 2 {
		panic("invalid offline CLPA arguments")
	}
	started := time.Now()
	f, err := os.Open(*input)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 1<<20)
	var n int64
	if err = binary.Read(r, binary.LittleEndian, &n); err != nil || n <= int64(*shards) {
		panic("invalid TRAIN account count")
	}
	params.ShardNum = *shards
	var graph partition.CLPAState
	graph.Init_CLPAState(0.5, 100, *shards)
	vertices := make([]partition.Vertex, n)
	address := make([]byte, 40)
	for i := range vertices {
		if _, err := io.ReadFull(r, address); err != nil {
			panic(err)
		}
		vertices[i] = partition.Vertex{Addr: string(address)}
		graph.AddVertex(vertices[i])
	}
	var edges int64
	if err = binary.Read(r, binary.LittleEndian, &edges); err != nil || edges < 0 {
		panic("invalid edge count")
	}
	var occurrences int64
	for i := int64(0); i < edges; i++ {
		var row struct {
			U, V  uint32
			Count int64
		}
		if err := binary.Read(r, binary.LittleEndian, &row); err != nil {
			panic(err)
		}
		if int64(row.U) >= n || int64(row.V) >= n || row.Count <= 0 || row.U == row.V {
			panic("invalid TRAIN edge")
		}
		for j := int64(0); j < row.Count; j++ {
			graph.AddEdge(vertices[row.U], vertices[row.V])
		}
		occurrences += row.Count
	}
	if _, err := r.ReadByte(); err != io.EOF {
		panic("unexpected trailing graph data")
	}
	fmt.Printf("graph_ready accounts=%d pairs=%d occurrences=%d elapsed=%s\n", n, edges, occurrences, time.Since(started))
	graph.CLPA_Partition()
	out, err := os.OpenFile(*output, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
	w := bufio.NewWriterSize(out, 1<<20)
	for _, vertex := range vertices {
		shard, exists := graph.PartitionMap[vertex]
		if !exists || shard < 0 || shard >= *shards {
			panic("invalid final CLPA route")
		}
		if err := binary.Write(w, binary.LittleEndian, int32(shard)); err != nil {
			panic(err)
		}
	}
	if err := w.Flush(); err != nil {
		panic(err)
	}
	if err := out.Close(); err != nil {
		panic(err)
	}
	fmt.Printf("CLPA_static_complete elapsed=%s\n", time.Since(started))
}
