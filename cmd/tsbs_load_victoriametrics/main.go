// tsbs_load_victoriametrics loads a VictoriaMetrics with data from stdin.
package main

import (
	"bufio"
	"bytes"
	"flag"
	"log"
	"net/http"
	"sync"

	"github.com/timescale/tsbs/load"
)

// Global vars
var (
	loader  *load.BenchmarkRunner
	bufPool sync.Pool
	vmURL   string
)

// allows for testing
var fatal = log.Fatalf

// Parse args:
func init() {
	bufPool = sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer(make([]byte, 0, 16*1024*1024))
		},
	}
	loader = load.GetBenchmarkRunner()
	flag.StringVar(&vmURL, "url", "http://localhost:8428/write", "VictoriaMetrics ingestion URL")
	flag.Parse()
}

// loader.Benchmark interface implementation
type benchmark struct{}

// loader.Benchmark interface implementation
func (b *benchmark) GetPointDecoder(br *bufio.Reader) load.PointDecoder {
	return &decoder{
		scanner: bufio.NewScanner(br),
	}
}

func (b *benchmark) GetBatchFactory() load.BatchFactory {
	return &factory{}
}

func (b *benchmark) GetPointIndexer(maxPartitions uint) load.PointIndexer {
	return &load.ConstantIndexer{}
}

func (b *benchmark) GetProcessor() load.Processor {
	return &processor{}
}

func (b *benchmark) GetDBCreator() load.DBCreator {
	return &dbCreator{}
}

func main() {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	loader.RunBenchmark(&benchmark{}, load.SingleQueue)
}
