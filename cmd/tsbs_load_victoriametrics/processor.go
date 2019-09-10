package main

import (
	"bytes"
	"log"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/timescale/tsbs/load"
)

type processor struct {
	workerPool chan *worker
}

func (p *processor) Init(workerNum int, _ bool) {
	if workerNum == 0 {
		workerNum = 1
	}
	client := &http.Client{
		Timeout: time.Minute,
	}
	p.workerPool = make(chan *worker, workerNum)
	for i := 0; i < workerNum; i++ {
		p.workerPool <- &worker{client}
	}
}

func (p *processor) ProcessBatch(b load.Batch, doLoad bool) (metricCount, rowCount uint64) {
	batch := b.(*batch)
	if !doLoad {
		return batch.metrics, batch.rows
	}

	w := <-p.workerPool
	mc, rc := w.do(batch)
	p.workerPool <- w
	return mc, rc
}

type worker struct {
	*http.Client
}

func (w *worker) do(b *batch) (uint64, uint64) {
	for {
		r := bytes.NewReader(b.buf.Bytes())
		req, err := http.NewRequest("POST", getURL(), r)
		if err != nil {
			log.Fatalf("error while creating new request: %s", err)
		}
		req.Header.Add("Content-Encoding", "snappy")
		req.Header.Set("Content-Type", "application/x-protobuf")
		resp, err := w.Do(req)
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

var cur int32

func getURL() string {
	if len(vmURLs) == 1 {
		return vmURLs[0]
	}
	idx := atomic.AddInt32(&cur, 1) % int32(len(vmURLs))
	return vmURLs[idx]
}
