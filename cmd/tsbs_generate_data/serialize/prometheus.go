package serialize

import (
	"io"
	"fmt"
	"sync"

	"github.com/prometheus/prometheus/prompb"
	"github.com/valyala/bytebufferpool"
	"github.com/klauspost/compress/snappy"
)

const PrometheusBatchSize = 1e4

// PrometheusSerializer writes a Point in a serialized form for Prometheus
type PrometheusSerializer struct {
	cur    int
	series []*prompb.TimeSeries
	w      io.Writer
}

// Flush flushes collected series into writer
func (s *PrometheusSerializer) Flush() error {
	wr := &prompb.WriteRequest{
		Timeseries: s.series[:s.cur],
	}
	data, err := wr.Marshal()
	if err != nil {
		return err
	}

	sb := bytebufferpool.Get()
	sb.B = snappy.Encode(sb.B, data)

	var sizeBuf []byte
	sizeBuf = marshalUint64(sizeBuf[:0], uint64(sb.Len()))
	if _, err := s.w.Write(sizeBuf); err != nil {
		bytebufferpool.Put(sb)
		return err
	}

	_, err = s.w.Write(sb.Bytes())
	s.cur = 0
	bytebufferpool.Put(sb)
	return err
}

func (s *PrometheusSerializer) Push(ts *prompb.TimeSeries) error {
	if s.cur > PrometheusBatchSize-1 {
		if err := s.Flush(); err != nil {
			return err
		}
	}
	s.series[s.cur] = ts
	s.cur++
	return nil
}

var once sync.Once

// Serialize writes Point data to the given writer, conforming to the
// Prometheus wire protocol.
func (s *PrometheusSerializer) Serialize(p *Point, w io.Writer) (err error) {
	once.Do(func() {
		s.w = w
		s.series = make([]*prompb.TimeSeries, PrometheusBatchSize)
	})

	labelsLen := len(p.tagKeys)
	labels := make([]*prompb.Label, labelsLen)
	if labelsLen > 0 {
		for i := 0; i < labelsLen; i++ {
			labels[i] = &prompb.Label{
				Name:  string(p.tagKeys[i]),
				Value: string(p.tagValues[i]),
			}
		}
	}
	prefix := string(p.measurementName)
	for i := 0; i < len(p.fieldKeys); i++ {
		ts := &prompb.TimeSeries{
			Labels:  labels,
			Samples: make([]prompb.Sample, 1),
		}
		labelName := &prompb.Label{
			Name:  "__name__",
			Value: fmt.Sprintf("%s_%s", prefix, string(p.fieldKeys[i])),
		}
		ts.Labels = append(ts.Labels, labelName)
		ts.Samples[0] = prompb.Sample{
			Timestamp: p.timestamp.UnixNano() / 1e6,
			Value:     toFloat64(p.fieldValues[i]),
		}
		if err := s.Push(ts); err != nil {
			return err
		}
	}
	return nil
}

func toFloat64(v interface{}) float64 {
	switch v.(type) {
	case int:
		return float64(v.(int))
	case int64:
		return float64(v.(int64))
	case float64:
		return v.(float64)
	case float32:
		return float64(v.(float32))
	default:
		panic(fmt.Sprintf("unknown field type for %#v", v))
	}
}

// marshalUint64 appends marshaled v to dst and returns the result.
func marshalUint64(dst []byte, u uint64) []byte {
	return append(dst, byte(u>>56), byte(u>>48), byte(u>>40), byte(u>>32), byte(u>>24), byte(u>>16), byte(u>>8), byte(u))
}
