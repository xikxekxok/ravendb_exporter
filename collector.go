package main

import (
	"regexp"
	"strconv"

	jp "github.com/buger/jsonparser"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	namespace = "ravendb"
	subsystem = ""
)

type exporter struct {
	up                         prometheus.Gauge
	workingSet                 prometheus.Gauge
	cpuTime                    prometheus.Counter
	isLeader                   prometheus.Gauge
	requestTotal               prometheus.Counter
	documentPutTotal           prometheus.Counter
	documentPutBytes           prometheus.Counter
	mapIndexIndexedTotal       prometheus.Counter
	mapReduceIndexMappedTotal  prometheus.Counter
	mapReduceIndexReducedTotal prometheus.Counter

	databaseDocuments    *prometheus.GaugeVec
	databaseIndexes      *prometheus.GaugeVec
	databaseStaleIndexes *prometheus.GaugeVec
	databaseSize         *prometheus.GaugeVec

	databaseRequestTotal               *prometheus.CounterVec
	databaseDocumentPutTotal           *prometheus.CounterVec
	databaseDocumentPutBytes           *prometheus.CounterVec
	databaseMapIndexIndexedTotal       *prometheus.CounterVec
	databaseMapReduceIndexMappedTotal  *prometheus.CounterVec
	databaseMapReduceIndexReducedTotal *prometheus.CounterVec
}

func newExporter() *exporter {
	return &exporter{
		up:                         createGauge("up", "Whether the RavenDB scrape was successful"),
		workingSet:                 createGauge("working_set_bytes", "Process working set"),
		cpuTime:                    createCounter("cpu_time_seconds_total", "CPU time"),
		isLeader:                   createGauge("is_leader", "If 1, then node is the cluster leader, otherwise 0"),
		requestTotal:               createCounter("request_total", "Server-wide request count"),
		documentPutTotal:           createCounter("document_put_total", "Server-wide document puts count"),
		documentPutBytes:           createCounter("document_put_bytes_total", "Server-wide document put bytes"),
		mapIndexIndexedTotal:       createCounter("mapindex_indexed_total", "Server-wide map index indexed count"),
		mapReduceIndexMappedTotal:  createCounter("mapreduceindex_mapped_total", "Server-wide map-reduce index mapped count"),
		mapReduceIndexReducedTotal: createCounter("mapreduceindex_reduced_total", "Server-wide map-reduce index reduced count"),

		databaseDocuments:    createDatabaseGaugeVec("database_documents", "Count of documents in a database"),
		databaseIndexes:      createDatabaseGaugeVec("database_indexes", "Count of indexes in a database"),
		databaseStaleIndexes: createDatabaseGaugeVec("database_stale_indexes", "Count of stale indexes in a database"),
		databaseSize:         createDatabaseGaugeVec("database_size_bytes", "Database size in bytes"),

		databaseRequestTotal:               createDatabaseCounterVec("database_request_total", "Database request count"),
		databaseDocumentPutTotal:           createDatabaseCounterVec("database_document_put_total", "Database document puts count"),
		databaseDocumentPutBytes:           createDatabaseCounterVec("database_document_put_bytes_total", "Database document put bytes"),
		databaseMapIndexIndexedTotal:       createDatabaseCounterVec("database_mapindex_indexed_total", "Database map index indexed count"),
		databaseMapReduceIndexMappedTotal:  createDatabaseCounterVec("database_mapreduceindex_mapped_total", "Database map-reduce index mapped count"),
		databaseMapReduceIndexReducedTotal: createDatabaseCounterVec("database_mapreduceindex_reduced_total", "Database map-reduce index reduced count"),
	}
}

func (e *exporter) Describe(ch chan<- *prometheus.Desc) {
	ch <- e.up.Desc()
	ch <- e.workingSet.Desc()
	ch <- e.cpuTime.Desc()
	ch <- e.isLeader.Desc()
	ch <- e.requestTotal.Desc()
	ch <- e.documentPutTotal.Desc()
	ch <- e.documentPutBytes.Desc()
	ch <- e.mapIndexIndexedTotal.Desc()
	ch <- e.mapReduceIndexMappedTotal.Desc()
	ch <- e.mapReduceIndexReducedTotal.Desc()

	e.databaseDocuments.Describe(ch)
	e.databaseIndexes.Describe(ch)
	e.databaseStaleIndexes.Describe(ch)
	e.databaseSize.Describe(ch)

	e.databaseRequestTotal.Describe(ch)
	e.databaseDocumentPutTotal.Describe(ch)
	e.databaseMapIndexIndexedTotal.Describe(ch)
	e.databaseMapReduceIndexMappedTotal.Describe(ch)
	e.databaseMapReduceIndexReducedTotal.Describe(ch)
}

func (e *exporter) Collect(ch chan<- prometheus.Metric) {
	log.Info("Running scrape")

	if stats, err := getStats(); err != nil {
		log.WithError(err).Error("Error while getting data from RavenDB")

		e.up.Set(0)
		ch <- e.up
	} else {
		e.up.Set(1)
		ch <- e.up

		e.workingSet.Set(getMemoryWorkingSet(stats))
		ch <- e.workingSet

		e.cpuTime.Add(getCPUTime(stats))
		ch <- e.cpuTime

		e.isLeader.Set(getIsLeader(stats))
		ch <- e.isLeader

		e.requestTotal.Add(getRequestTotal(stats))
		ch <- e.requestTotal

		e.documentPutTotal.Add(getDocumentPutTotal(stats))
		ch <- e.documentPutTotal

		e.documentPutBytes.Add(getDocumentPutBytesTotal(stats))
		ch <- e.documentPutBytes

		e.mapIndexIndexedTotal.Add(getMapIndexIndexedTotal(stats))
		ch <- e.mapIndexIndexedTotal

		e.mapReduceIndexMappedTotal.Add(getMapReduceIndexMappedTotal(stats))
		ch <- e.mapReduceIndexMappedTotal

		e.mapReduceIndexReducedTotal.Add(getMapReduceIndexReducedTotal(stats))
		ch <- e.mapReduceIndexReducedTotal

		collectPerDatabaseGauge(stats, e.databaseDocuments, getDatabaseDocuments, ch)
		collectPerDatabaseGauge(stats, e.databaseIndexes, getDatabaseIndexes, ch)
		collectPerDatabaseGauge(stats, e.databaseStaleIndexes, getDatabaseStaleIndexes, ch)
		collectPerDatabaseGauge(stats, e.databaseSize, getDatabaseSize, ch)

		collectPerDatabaseCounter(stats, e.databaseRequestTotal, getDatabaseRequestTotal, ch)
		collectPerDatabaseCounter(stats, e.databaseDocumentPutBytes, getDatabaseDocumentPutBytes, ch)
		collectPerDatabaseCounter(stats, e.databaseDocumentPutTotal, getDatabaseDocumentPutTotal, ch)

		collectPerDatabaseCounter(stats, e.databaseMapIndexIndexedTotal, getDatabaseMapIndexIndexedTotal, ch)
		collectPerDatabaseCounter(stats, e.databaseMapReduceIndexMappedTotal, getDatabaseMapReduceIndexMappedTotal, ch)
		collectPerDatabaseCounter(stats, e.databaseMapReduceIndexReducedTotal, getDatabaseMapReduceIndexReducedTotal, ch)

	}
}

func collectPerDatabaseGauge(stats *stats, vec *prometheus.GaugeVec, collectFunc func(*dbStats) float64, ch chan<- prometheus.Metric) {
	for _, dbs := range stats.dbStats {
		vec.WithLabelValues(dbs.database).Set(collectFunc(dbs))
	}
	vec.Collect(ch)
}

func collectPerDatabaseCounter(stats *stats, vec *prometheus.CounterVec, collectFunc func(*dbStats) float64, ch chan<- prometheus.Metric) {
	for _, dbs := range stats.dbStats {
		vec.WithLabelValues(dbs.database).Add(collectFunc(dbs))
	}
	vec.Collect(ch)
}

func getCPUTime(stats *stats) float64 {
	var cpuTimeString string
	jp.ArrayEach(stats.cpu, func(value []byte, dataType jp.ValueType, offset int, err error) {
		cpuTimeString, _ = jp.GetString(value, "TotalProcessorTime") // just use the last entry in the array TODO: why is this an array?
	}, "CpuStats")

	return timeSpanToSeconds(cpuTimeString)
}

func getMemoryWorkingSet(stats *stats) float64 {
	value, _ := jp.GetFloat(stats.memory, "WorkingSet")
	return value
}

func getIsLeader(stats *stats) float64 {
	value, _ := jp.GetString(stats.nodeInfo, "CurrentState")
	if value == "Leader" {
		return 1
	}
	return 0
}

func getRequestTotal(stats *stats) float64 {
	value, _ := jp.GetFloat(stats.metrics, "Requests", "RequestsPerSec", "Count")
	return value
}

func getDocumentPutTotal(stats *stats) float64 {
	value, _ := jp.GetFloat(stats.metrics, "Docs", "PutsPerSec", "Count")
	return value
}

func getDocumentPutBytesTotal(stats *stats) float64 {
	value, _ := jp.GetFloat(stats.metrics, "Docs", "BytesPutsPerSec", "Count")
	return value
}

func getMapIndexIndexedTotal(stats *stats) float64 {
	value, _ := jp.GetFloat(stats.metrics, "MapIndexes", "MappedPerSec", "Count")
	return value
}

func getMapReduceIndexMappedTotal(stats *stats) float64 {
	value, _ := jp.GetFloat(stats.metrics, "MapReduceIndexes", "MappedPerSec", "Count")
	return value
}

func getMapReduceIndexReducedTotal(stats *stats) float64 {
	value, _ := jp.GetFloat(stats.metrics, "MapReduceIndexes", "ReducedPerSec", "Count")
	return value
}

func getDatabaseDocuments(dbStats *dbStats) float64 {
	value, _ := jp.GetFloat(dbStats.databaseStats, "CountOfDocuments")
	return value
}

func getDatabaseIndexes(dbStats *dbStats) float64 {
	value, _ := jp.GetFloat(dbStats.databaseStats, "CountOfIndexes")
	return value
}

func getDatabaseStaleIndexes(dbStats *dbStats) float64 {
	count := 0
	jp.ArrayEach(dbStats.databaseStats, func(value []byte, dataType jp.ValueType, offset int, err error) {
		if isStale, _ := jp.GetBoolean(value, "IsStale"); isStale {
			count++
		}
	}, "Indexes")

	return float64(count)
}

func getDatabaseSize(dbStats *dbStats) float64 {
	value, _ := jp.GetFloat(dbStats.databaseStats, "SizeOnDisk", "SizeInBytes")
	return value
}

func getDatabaseRequestTotal(dbStats *dbStats) float64 {
	value, _ := jp.GetFloat(dbStats.metrics, "Requests", "RequestsPerSec", "Count")
	return value
}

func getDatabaseDocumentPutTotal(dbStats *dbStats) float64 {
	value, _ := jp.GetFloat(dbStats.metrics, "Docs", "PutsPerSec", "Count")
	return value
}

func getDatabaseDocumentPutBytes(dbStats *dbStats) float64 {
	value, _ := jp.GetFloat(dbStats.metrics, "Docs", "BytesPutsPerSec", "Count")
	return value
}

func getDatabaseMapIndexIndexedTotal(dbStats *dbStats) float64 {
	value, _ := jp.GetFloat(dbStats.metrics, "MapIndexes", "IndexedPerSec", "Count")
	return value
}

func getDatabaseMapReduceIndexMappedTotal(dbStats *dbStats) float64 {
	value, _ := jp.GetFloat(dbStats.metrics, "MapIndexes", "MappedPerSec", "Count")
	return value
}

func getDatabaseMapReduceIndexReducedTotal(dbStats *dbStats) float64 {
	value, _ := jp.GetFloat(dbStats.metrics, "MapIndexes", "ReducedPerSec", "Count")
	return value
}

func createGauge(name string, help string) prometheus.Gauge {
	return prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      name,
		Help:      help,
	})
}

func createDatabaseGaugeVec(name string, help string) *prometheus.GaugeVec {
	return prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      name,
		Help:      help,
	}, []string{"database"})
}

func createCounter(name string, help string) prometheus.Counter {
	return prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      name,
		Help:      help,
	})
}

func createDatabaseCounterVec(name string, help string) *prometheus.CounterVec {
	return prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      name,
		Help:      help,
	}, []string{"database"})
}

var timespanRegex = regexp.MustCompile(`((?P<days>\d+)\.)?(?P<hours>\d{2}):(?P<minutes>\d{2}):(?P<seconds>\d{2})(\.(?P<secondfraction>\d{7}))?`)

func timeSpanToSeconds(timespanString string) float64 {

	var result float64

	matches := matchNamedGroups(timespanRegex, timespanString)
	if daysString, ok := matches["days"]; ok {
		days, _ := strconv.Atoi(daysString)
		result = result + float64(days)*24*60*60
	}
	if hoursString, ok := matches["hours"]; ok {
		hours, _ := strconv.Atoi(hoursString)
		result = result + float64(hours)*60*60
	}
	if minutesString, ok := matches["minutes"]; ok {
		minutes, _ := strconv.Atoi(minutesString)
		result = result + float64(minutes)*60
	}
	if secondsString, ok := matches["seconds"]; ok {
		seconds, _ := strconv.Atoi(secondsString)
		result = result + float64(seconds)
	}
	if secondFractionString, ok := matches["secondfraction"]; ok {
		secondFraction, _ := strconv.Atoi(secondFractionString)
		result = result + float64(secondFraction)/10000000
	}

	return result
}

func matchNamedGroups(regex *regexp.Regexp, text string) map[string]string {
	matches := regex.FindStringSubmatch(text)

	results := make(map[string]string)
	for i, name := range regex.SubexpNames() {
		if name != "" {
			results[name] = matches[i]
		}
	}
	return results
}
