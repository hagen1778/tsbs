package main

import (
	"bytes"
	"log"
	"net/http"
	"time"

	"github.com/timescale/tsbs/load"
)

type processor struct {
	*http.Client
	url string
}

func (p *processor) Init(workerNum int, _ bool) {
	p.Client = &http.Client{
		Timeout: time.Minute,
	}
	p.url = vmURLs[workerNum%len(vmURLs)]
}

func (p *processor) ProcessBatch(b load.Batch, doLoad bool) (metricCount, rowCount uint64) {
	batch := b.(*batch)
	if !doLoad {
		return batch.metrics, batch.rows
	}
	mc, rc := p.do(batch)
	return mc, rc
}

func (p *processor) do(b *batch) (uint64, uint64) {
	for {
		r := bytes.NewReader(b.buf.Bytes())
		req, err := http.NewRequest("POST", p.url, r)
		if err != nil {
			log.Fatalf("error while creating new request: %s", err)
		}
		req.Header.Add("Content-Encoding", "snappy")
		req.Header.Set("Content-Type", "application/x-protobuf")
		resp, err := p.Do(req)
		if err != nil {
			log.Fatalf("error while executing request: %s", err)
		}
		resp.Body.Close()
		if resp.StatusCode == http.StatusNoContent {
			b.buf.Reset()
			return b.metrics, b.rows
		}
		log.Printf("server returned HTTP status %d. Retrying", resp.StatusCode)
		time.Sleep(time.Millisecond * 10)
	}
}
