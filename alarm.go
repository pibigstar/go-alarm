package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"reflect"
	"syscall"
	"time"

	"github.com/olivere/elastic/v7"
	"github.com/robfig/cron"
)

const (
	host      = "http://172.16.0.28:9200"
	IndexDev  = "logstash-dolphin-dev-%s"
	IndexProd = "logstash-dolphin-prod-%s"
)

type MyError struct {
	KeyWork  string
	ID       []string
	Total    int
	Contents interface{}
}

type Content struct {
	Tid    string `json:"tid,omitempty"`
	Level  string `json:"level,omitempty"`
	Svc    string `json:"svc,omitempty"`
	Method string `json:"method,omitempty"`
}

type Alarm interface {
	send(err *MyError)
}

type AlarmError struct {
	alarms []Alarm
}

type DingDing struct {
}

func (*DingDing) send(err *MyError) {
	fmt.Printf("出现了: 【%s】 错误 总计: %d 次 日志ID: %s 日志内容: %+v \n", err.KeyWork, err.Total, err.ID, err.Contents)
}

var client *esClient

type esClient struct {
	*elastic.Client
	ctx context.Context
}

// init the elastic search
func init() {
	err := NewElasticSearchClient()
	if err != nil {
		panic(err)
	}
}

// search the result by query strings
func (client *esClient) Query(index string, keyword string) (*elastic.SearchResult, error) {

	agg := elastic.NewDateHistogramAggregation().
		Field("@timestamp").
		TimeZone("Asia/Shanghai").
		MinDocCount(1).
		Interval("1m")

	boolQuery := elastic.NewBoolQuery().
		Filter(elastic.NewRangeQuery("@timestamp").
			Format("strict_date_optional_time").
			Gte(time.Now().Add(time.Minute * -1).Format(time.RFC3339)).
			Lte(time.Now().Format(time.RFC3339))).
		Filter(elastic.NewBoolQuery().
			MinimumNumberShouldMatch(1).
			Should(elastic.NewMatchPhraseQuery("code", keyword)))

	result, err := client.Search().
		Index(index).
		Query(boolQuery).
		Timeout("30000ms").
		Size(500).
		Aggregation("aggs", agg).
		Version(true).
		StoredFields("*").
		Do(client.ctx)

	return result, err
}

func main() {
	a := &AlarmError{}

	dingding := &DingDing{}
	a.alarms = append(a.alarms, dingding)

	c := cron.New()
	c.AddFunc("@every 1m", func() {
		a.findError("10404")
	})

	c.Start()

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, os.Kill, syscall.SIGTERM)
	<-interrupt
}

func (a *AlarmError) findError(errors ...string) {
	for _, errStr := range errors {
		go func(errStr string) {

			date := time.Now().Format("2006.01.02")

			result, err := client.Query(fmt.Sprintf(IndexDev, date), errStr)
			if err != nil {
				panic(err)
			}

			if result.TotalHits() > 0 {

				var content Content
				docs := result.Each(reflect.TypeOf(content))

				var ids []string
				for _, hit := range result.Hits.Hits {
					ids = append(ids, hit.Id)
				}

				myErr := &MyError{
					KeyWork:  errStr,
					Total:    len(docs),
					ID:       ids,
					Contents: docs,
				}

				for _, alarm := range a.alarms {
					alarm.send(myErr)
				}

			}

		}(errStr)
	}
}

// Build the elasticSearch the client
func NewElasticSearchClient() error {
	esCli, err := elastic.NewClient(elastic.SetURL(host))
	if err != nil {
		return err
	}
	client = &esClient{Client: esCli, ctx: context.Background()}
	result, code, err := client.Ping(host).Do(context.Background())
	if err != nil {
		return err
	}
	fmt.Printf("Elasticsearch returned with code: %d and version: %s \n", code, result.Version.Number)

	return nil
}
