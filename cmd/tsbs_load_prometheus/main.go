package main

import (
	"bufio"
	"flag"
	"log"
	"time"
	"net/http"
	"fmt"
	"bytes"

	"github.com/hagen1778/tsbs/load"
)

var (
	loader           *load.BenchmarkRunner
	remoteStorageURL string
)

func init() {
	loader = load.GetBenchmarkRunner()
	flag.StringVar(&remoteStorageURL, "url", "http://localhost:8080", "Prometheus Remote Storage Insert daemon URL")
	flag.Parse()
	remoteStorageURL = fmt.Sprintf("%s/insert/1d/1/foobar/prometheus/", remoteStorageURL)
}

type benchmark struct{}

func (b *benchmark) GetPointDecoder(br *bufio.Reader) load.PointDecoder {
	return &decoder{scanner: &scanner{r: br}}
}

func (b *benchmark) GetBatchFactory() load.BatchFactory {
	return &factory{}
}

func (b *benchmark) GetPointIndexer(_ uint) load.PointIndexer {
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

type processor struct {
	*http.Client
}

func (p *processor) Init(numWorker int, _ bool) {
	p.Client = &http.Client{
		Timeout: time.Minute,
	}
}

func (p *processor) Close(_ bool) {}

func (p *processor) ProcessBatch(b load.Batch, doLoad bool) (uint64, uint64) {
	batch := b.(*batch)
	if !doLoad || len(batch.Bytes()) == 0 {
		return 0, 0
	}

	for {
		httpReq, err := http.NewRequest("POST", remoteStorageURL, bytes.NewReader(batch.Bytes()))
		if err != nil {
			log.Fatalf("error while creating new request: %s", err)
		}
		httpReq.Header.Add("Content-Encoding", "snappy")
		httpReq.Header.Set("Content-Type", "application/x-protobuf")
		httpReq.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")

		httpResp, err := p.Client.Do(httpReq)
		if err != nil {
			log.Fatalf("error while executing request: %s", err)
		}
		httpResp.Body.Close()
		if httpResp.StatusCode == http.StatusOK {
			return uint64(batch.Len()), 0
		}
		log.Printf("server returned HTTP status %d. Retrying", httpResp.Status)
		time.Sleep(time.Millisecond*10)
	}
}
