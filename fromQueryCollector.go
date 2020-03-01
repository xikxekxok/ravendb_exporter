package main

import (
	"errors"
	"fmt"
	jp "github.com/buger/jsonparser"
	"github.com/prometheus/client_golang/prometheus"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"net/url"
	"os"
	"strings"
	"time"
)

type Query struct {
	Name         string
	RQL          string
	Database     string
	ValueOnError float64  `yaml:"value-on-error"`
	ValueField   string   `yaml:"value-field"`
	LabelFields  []string `yaml:"label-fields"`
	Interval     time.Duration
}

func startQueryCollector(queryDirPath string) {
	queries, err := loadQueriesFromDir(queryDirPath)
	if err != nil {
		log.Fatal(err)
	}
	for _, query := range queries {
		metric := createMetricObj(query)
		go runQueryCollector(query, metric)
	}
}

func loadQueriesFromDir(path string) ([]Query, error) {
	result := make([]Query, 0)
	if path == "" {
		log.Info("QueriesDir not passed")
		return result, nil
	}
	log.Info("Load queries from directory ", path)
	files, err := ioutil.ReadDir(path)
	if err != nil {
		return nil, err
	}

	for _, f := range files {
		fn := f.Name()
		if strings.HasSuffix(fn, ".yml") {
			fn := fmt.Sprintf("%s/%s", strings.TrimRight(path, "/"), fn)
			log.Println("Loading", fn)
			file, err := os.Open(fn)
			if err != nil {
				return nil, err
			}
			defer file.Close()

			b, err := ioutil.ReadAll(file)
			if err != nil {
				return nil, err
			}
			fromFileQueries := make([]Query, 0)
			if err = yaml.Unmarshal(b, &fromFileQueries); err != nil {
				return nil, err
			}
			for _, query := range fromFileQueries {
				if query.Name == "" {
					return nil, errors.New("Query name missed!")
				}
				if query.Database == "" {
					return nil, fmt.Errorf("No database specified for query [%s]", query.Name)
				}
				if query.RQL == "" {
					return nil, fmt.Errorf("RQL statement required for query [%s]", query.Name)
				}
				if query.Interval == 0 {
					return nil, fmt.Errorf("Interval must be greater than zero for query [%s]", query.Name)
				}
				if query.ValueField == "" {
					return nil, fmt.Errorf("ValueField required for query [%s]", query.Name)
				}
				result = append(result, query)
			}
		}
	}
	return result, nil
}

func runQueryCollector(query Query, metric *prometheus.GaugeVec) {
	for {
		log.Info("running query ", query.Name)
		err := readAndSetMetrics(&query, metric)
		if err != nil {
			log.WithError(err).Error("Error while executing query " + query.Name)
			metric.WithLabelValues().Set(query.ValueOnError)
		} else {
			log.Info("Metrics from query " + query.Name + " collected!")
		}
		select {
		case <-time.After(query.Interval):
			continue
		}
	}
}

func createMetricObj(query Query) *prometheus.GaugeVec {
	metric := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: fmt.Sprintf("ravendb_queryresult_%s", query.Name),
		Help: "Result of an RQL query " + query.Name,
	}, query.LabelFields)
	prometheus.MustRegister(metric)
	return metric
}

func readAndSetMetrics(query *Query, metric *prometheus.GaugeVec) error {

	response, err := get(fmt.Sprintf("/databases/%s/queries?query=%s&start=0&pageSize=101&metadataOnly=false", query.Database, url.QueryEscape(query.RQL)))
	if err != nil {
		log.WithError(err).Error("Error while executing query ", query.Name)

		return err
	}
	var globalErr error = nil
	jp.ArrayEach(response, func(node []byte, dataType jp.ValueType, offset int, err error) {
		value, err := jp.GetFloat(node, query.ValueField)
		if err != nil {
			log.WithError(err).Error("Cannot read metric value from RavenDb response!")
			globalErr = err
			return
		}
		if query.LabelFields != nil {
			labelValues := make(map[string]string, 0)
			for _, label := range query.LabelFields {
				labelValue, err := jp.GetString(node, label)
				if err != nil {
					log.WithError(err).Error("Cannot read metric label from RavenDb response!")
					globalErr = err
					return
				}
				labelValues[label] = labelValue
			}

			metric.With(labelValues).Set(value)
		} else {
			metric.WithLabelValues().Set(value)
		}
	}, "Results")
	return globalErr
}
