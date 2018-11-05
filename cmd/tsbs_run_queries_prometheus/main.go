package main

import (
	"flag"
	"net/http"
	"time"
	"io/ioutil"
	"encoding/json"
	"fmt"

	"github.com/hagen1778/tsbs/query"
)

// Program option vars:
var url string

// Global vars:
var runner *query.BenchmarkRunner

func init() {
	runner = query.NewBenchmarkRunner()
	flag.StringVar(&url, "url", "http://localhost:8081/select/1m/1/foobar/prometheus/", "Prometheus URL")
	flag.Parse()
}

func main() {
	runner.Run(&query.HTTPPool, newProcessor)
}

type processor struct {
	*http.Client
}

func newProcessor() query.Processor { return &processor{} }

func (p *processor) Init(workerNumber int) {
}

type response struct {
	Status string `json:"status"`
	Data struct {
		Result []result `json:"result"`
	} `json:"data"`
}

type result struct {
	Metric interface{} `json:"metric"`
	Values []interface{} `json:"values"`
}

func (p *processor) ProcessQuery(q query.Query, _ bool) ([]*query.Stat, error) {
	hq := q.(*query.HTTP)

	// populate a request with data from the Query:
	req, err := http.NewRequest(string(hq.Method), url+string(hq.Path), nil)
	if err != nil {
		return nil, fmt.Errorf("error while creating new request: %s", err)
	}

	// Perform the request while tracking latency:
	start := time.Now()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("non-200 statuscode received: %d", resp.StatusCode)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error while reading response body: %s", err)
	}

	r := response{}
	if err := json.Unmarshal(body, &r); err != nil {
		return nil, fmt.Errorf("error while unmarshaling response: %s", err)
	}

	lag := float64(time.Since(start).Nanoseconds()) / 1e6 // milliseconds
	stat := query.GetStat()
	stat.Init(q.HumanLabelName(), lag)
	return []*query.Stat{stat}, nil
}
