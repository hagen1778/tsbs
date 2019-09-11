// tsbs_load_victoriametrics loads a VictoriaMetrics with data from stdin.
package main

import (
	"bufio"
	"bytes"
	"flag"
	"log"
	"strings"
	"sync"

	"github.com/timescale/tsbs/load"
)

// Global vars
var (
	loader  *load.BenchmarkRunner
	bufPool sync.Pool
	vmURLs  []string
)

// Parse args:
func init() {
	bufPool = sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer(make([]byte, 0, 16*1024*1024))
		},
	}
	loader = load.GetBenchmarkRunner()
	var urls string
	flag.StringVar(&urls, "urls", "http://localhost:8428/write",
		"Comma-separated list of VictoriaMetrics ingestion URLs(single-node or VMInsert)")
	flag.Parse()

	if len(urls) == 0 {
		log.Fatalf("missing `urls` flag")
	}
	vmURLs = strings.Split(urls, ",")
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
	loader.RunBenchmark(&benchmark{}, load.SingleQueue)
}
