package prometheus

import (
	"fmt"
	"net/url"
	"strings"
	"time"
	"strconv"

	"github.com/hagen1778/tsbs/cmd/tsbs_generate_queries/uses/devops"
	"github.com/hagen1778/tsbs/query"
)

// Devops produces Influx-specific queries for all the devops query types.
type Devops struct {
	*devops.Core
}

// NewDevops makes an Devops object ready to generate Queries.
func NewDevops(start, end time.Time, scale int) *Devops {
	return &Devops{devops.NewCore(start, end, scale)}
}

// GenerateEmptyQuery returns an empty query.HTTP
func (d *Devops) GenerateEmptyQuery() query.Query {
	return query.NewHTTP()
}

func getHostClause(hostnames []string) string {
	if len(hostnames) == 0 {
		return ""
	}
	if len(hostnames) == 1 {
		return fmt.Sprintf("hostname='%s'", hostnames[0])
	}

	return fmt.Sprintf("hostname=~'%s'", strings.Join(hostnames, "|"))
}

func getSelectClause(metrics, hosts []string) string {
	if len(metrics) == 0 {
		panic("BUG: must be at least one metric name in clause")
	}
	metricsCPU := make([]string, len(metrics))
	// concat with measurementName to get full metric name
	for i, v := range metrics {
		metricsCPU[i] = fmt.Sprintf("cpu_%s", v)
	}

	hostClause := getHostClause(hosts)
	if len(metrics) == 1 {
		return fmt.Sprintf("%s{%s}", metricsCPU[0], hostClause)
	}

	metricsClause := strings.Join(metricsCPU, "|")
	if len(hosts) > 0 {
		return fmt.Sprintf("{__name__=~%q, %s}", metricsClause, hostClause)
	}
	return fmt.Sprintf("{__name__=~%q}", metricsClause)
}

type queryInfo struct {
	// prometheus query
	query string
	// label to describe type of query
	label string
	// time range for query executing
	timeRange time.Duration
	// period of time to group by in seconds
	step string
}

// GroupByTime selects the MAX for numMetrics metrics under 'cpu' for nhosts hosts,
// e.g.:
// max(max_over_time({__name__=~"metric1|metric2...|metricN",hostname=~"hostname1|hostname2...|hostnameN"})) by (__name__)
func (d *Devops) GroupByTime(qq query.Query, nHosts, numMetrics int, timeRange time.Duration) {
	metrics := devops.GetCPUMetricsSlice(numMetrics)
	hosts := d.GetRandomHosts(nHosts)
	selectClause := getSelectClause(metrics, hosts)
	qi := &queryInfo{
		query:     fmt.Sprintf("max(max_over_time(%s)) by (__name__)", selectClause),
		label:     fmt.Sprintf("Prometheus %d cpu metric(s), random %4d hosts, random %s by 1m", numMetrics, nHosts, timeRange),
		timeRange: timeRange,
		step:      "60",
	}
	d.fillInQuery(qq, qi)
}

// GroupByTimeAndPrimaryTag selects the AVG of numMetrics metrics under 'cpu' per device per hour for a day,
// e.g. in psuedo-SQL:
//
// avg(avg_over_time({__name__=~"metric1|metric2...|metricN"})) by (__name__, hostname)
func (d *Devops) GroupByTimeAndPrimaryTag(qq query.Query, numMetrics int) {
	metrics := devops.GetCPUMetricsSlice(numMetrics)
	selectClause := getSelectClause(metrics, []string{})
	qi := &queryInfo{
		query:     fmt.Sprintf("avg(avg_over_time(%s)) by (__name__, hostname)", selectClause),
		label:     devops.GetDoubleGroupByLabel("Prometheus", numMetrics),
		timeRange: devops.DoubleGroupByDuration,
		step:      "3600",
	}
	d.fillInQuery(qq, qi)
}

// MaxAllCPU selects the MAX of all metrics under 'cpu' per hour for nhosts hosts,
// e.g.:
//
// max(max_over_time({hostname=~"hostname1|hostname2...|hostnameN"}))
func (d *Devops) MaxAllCPU(qq query.Query, nHosts int) {
	hosts := d.GetRandomHosts(nHosts)
	selectClause := getSelectClause(nil, hosts)
	qi := &queryInfo{
		query:     fmt.Sprintf("max(max_over_time(%s)) by (__name__)", selectClause),
		label:     devops.GetMaxAllLabel("Prometheus", nHosts),
		timeRange: devops.MaxAllDuration,
		step:      "3600",
	}
	d.fillInQuery(qq, qi)
}

// HighCPUForHosts populates a query that gets CPU metrics when the CPU has high
// usage between a time period for a number of hosts (if 0, it will search all hosts),
// e.g.:
//
// max(max_over_time(cpu_usage_user{hostname=~"hostname1|hostname2...|hostnameN"})) by (hostname) > 90
func (d *Devops) HighCPUForHosts(qq query.Query, nHosts int) {
	metrics := devops.GetCPUMetricsSlice(1)
	var hosts []string
	if nHosts > 0 {
		hosts = d.GetRandomHosts(nHosts)
	}
	selectClause := getSelectClause(metrics, hosts)
	qi := &queryInfo{
		query:     fmt.Sprintf("max(max_over_time(%s)) by (hostname) > 90", selectClause),
		label:     devops.GetMaxAllLabel("Prometheus", nHosts),
		timeRange: devops.HighCPUDuration,
		step:      fmt.Sprintf("%d", devops.HighCPUDuration),
	}
	d.fillInQuery(qq, qi)
}

func (d *Devops) fillInQuery(qq query.Query, qi *queryInfo) {
	interval := d.Interval.RandWindow(qi.timeRange)
	humanDesc := fmt.Sprintf("%s: %s", qi.label, interval.StartString())

	v := url.Values{}
	v.Set("query", qi.query)
	v.Set("start", strconv.FormatInt(interval.StartUnixNano()/1e9, 10))
	v.Set("end", strconv.FormatInt(interval.EndUnixNano()/1e9, 10))
	v.Set("step", qi.step)

	q := qq.(*query.HTTP)
	q.HumanLabel = []byte(qi.label)
	q.HumanDescription = []byte(humanDesc)
	q.Method = []byte("GET")
	q.Path = []byte(fmt.Sprintf("/api/v1/query_range?%s", v.Encode()))
	q.Body = nil
}
