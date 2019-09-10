package victoriametrics

import (
	"fmt"
	"strings"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/devops"
	"github.com/timescale/tsbs/query"
)

// Devops produces PromQL queries for all the devops query types.
type Devops struct {
	*BaseGenerator
	*devops.Core
}

func (d *Devops) GroupByOrderByLimit(qi query.Query) {
	panic("GroupByOrderByLimit not supported in PromQL")
}

// LastPointPerHost finds the last row for every host in the dataset
// e.g. in pseudo-PromQL:
// sum({__name__=~"cpu_.*"}) by(hostname)
func (d *Devops) LastPointPerHost(qq query.Query) {
	qi := &queryInfo{
		query:   "sum({__name__=~'cpu_.*'}) by(hostname)",
		label:   "VictoriaMetrics last point per host",
		instant: true,
	}
	d.fillInQuery(qq, qi)
}

// GroupByTime selects the MAX for numMetrics metrics under 'cpu'
// per minute for nhosts hosts,
// e.g. in pseudo-PromQL:
// max(
// 	max_over_time(
// 		{__name__=~"metric1|metric2...|metricN",hostname=~"hostname1|hostname2...|hostnameN"}[1m]
// 	)
// ) by (__name__)
func (d *Devops) GroupByTime(qq query.Query, nHosts, numMetrics int, timeRange time.Duration) {
	metrics := devops.MustGetCPUMetricsSlice(numMetrics)
	hosts := d.MustGetRandomHosts(nHosts)
	selectClause := getSelectClause(metrics, hosts)
	qi := &queryInfo{
		query:    fmt.Sprintf("max(max_over_time(%s)) by (__name__)", selectClause),
		label:    fmt.Sprintf("VictoriaMetrics %d cpu metric(s), random %4d hosts, random %s by 1m", numMetrics, nHosts, timeRange),
		interval: d.Interval.MustRandWindow(timeRange),
		step:     "60",
	}
	d.fillInQuery(qq, qi)
}

// GroupByTimeAndPrimaryTag selects the AVG of numMetrics metrics under 'cpu' per device per hour for a day,
// e.g. in pseudo-PromQL:
//
// avg(
// 	avg_over_time(
// 		{__name__=~"metric1|metric2...|metricN"}[1h]
// 	)
// ) by (hostname)
func (d *Devops) GroupByTimeAndPrimaryTag(qq query.Query, numMetrics int) {
	metrics := devops.MustGetCPUMetricsSlice(numMetrics)
	selectClause := getSelectClause(metrics, nil)
	qi := &queryInfo{
		query:    fmt.Sprintf("avg(avg_over_time(%s[1h])) by (__name__, hostname)", selectClause),
		label:    devops.GetDoubleGroupByLabel("VictoriaMetrics", numMetrics),
		interval: d.Interval.MustRandWindow(devops.DoubleGroupByDuration),
		step:     "3600",
	}
	d.fillInQuery(qq, qi)
}

// MaxAllCPU selects the MAX of all metrics under 'cpu' per hour for nhosts hosts,
// e.g. in pseudo-PromQL:
//
// max(
// 	max_over_time(
// 		{hostname=~"hostname1|hostname2...|hostnameN"}[1h]
// 	)
// )
func (d *Devops) MaxAllCPU(qq query.Query, nHosts int) {
	hosts := d.MustGetRandomHosts(nHosts)
	selectClause := getSelectClause(devops.GetAllCPUMetrics(), hosts)
	qi := &queryInfo{
		query:    fmt.Sprintf("max(max_over_time(%s[1h])) by (__name__)", selectClause),
		label:    devops.GetMaxAllLabel("VictoriaMetrics", nHosts),
		interval: d.Interval.MustRandWindow(devops.MaxAllDuration),
		step:     "3600",
	}
	d.fillInQuery(qq, qi)
}

// HighCPUForHosts populates a query that gets CPU metrics when the CPU has high
// usage between a time period for a number of hosts (if 0, it will search all hosts),
// e.g. in pseudo-PromQL:
//
// max(
// 	{__name__=~'cpu_.*', hostname=~"hostname1|hostname2...|hostnameN"}
// ) by (cpu_usage_user) > 90
func (d *Devops) HighCPUForHosts(qq query.Query, nHosts int) {
	var hostClause string
	if nHosts > 0 {
		hosts := d.MustGetRandomHosts(nHosts)
		hostClause = getHostClause(hosts)
	}
	qi := &queryInfo{
		query:    fmt.Sprintf("max({__name__=~'cpu_.*'%s}) by (cpu_usage_user) > 90", hostClause),
		label:    devops.GetMaxAllLabel("Victoria etrics", nHosts),
		interval: d.Interval.MustRandWindow(devops.HighCPUDuration),
		step:     fmt.Sprintf("%f.0", devops.HighCPUDuration.Seconds()),
	}
	d.fillInQuery(qq, qi)
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

	hostsClause := getHostClause(hosts)
	if len(metrics) == 1 {
		return fmt.Sprintf("cpu_%s{%s}", metrics[0], hostsClause)
	}

	metricsClause := strings.Join(metrics, "|")
	if len(hosts) > 0 {
		return fmt.Sprintf("{__name__=~'cpu_(%s)', %s}", metricsClause, hostsClause)
	}
	return fmt.Sprintf("{__name__=~'cpu_(%s)'", metricsClause)
}
