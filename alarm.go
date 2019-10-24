package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/olivere/elastic/v7"
	"github.com/robfig/cron"
)

const host = "http://172.16.0.28:9200"

type MyError struct {
	KeyWork  string
	ID       string
	Contents []Content
}

type Content struct {
	IP     string `json:"ip,omitempty"`
	Level  string `json:"level,omitempty"`
	Method string `json:"method,omitempty"`
	Msg    string `json:"msg,omitempty"`
	Ns     string `json:"ns,omitempty"`
	Org    string `json:"org,omitempty"`
	Rol    string `json:"role,omitempty"`
	Svc    string `json:"svc,omitempty"`
	Tid    string `json:"tid,omitempty"`
	Type   string `json:"type,omitempty"`
	User   string `json:"user,omitempty"`
}

type Alarm interface {
	send(err MyError)
}

type AlarmError struct {
	alarms []Alarm
}

type DingDing struct {
}

func (*DingDing) send(err MyError) {
	fmt.Printf("出现了: 【%s】 错误, 日志ID: %s \n", err.KeyWork, err.ID)
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
			Gte(time.Now().Add(time.Hour * -3).Format(time.RFC3339)).
			Lte(time.Now().Format(time.RFC3339))).
		Filter(elastic.NewMultiMatchQuery(keyword).
			Type("best_fields").
			Lenient(true)).
		Filter(elastic.NewBoolQuery().
			MinimumNumberShouldMatch(1).
			Should(elastic.NewMatchQuery("tid", keyword)))

	result, err := client.Search().
		Index(index).
		Query(boolQuery).
		Timeout("30000ms").
		IgnoreUnavailable(true).
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
		a.findError("2ab84762e15cdcc9bb723993b672281b")
	})

	c.Start()

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, os.Kill, syscall.SIGTERM)
	<-interrupt
}

func (a *AlarmError) findError(errors ...string) {
	for _, errStr := range errors {
		go func(errStr string) {
			result, err := client.Query("logstash-dolphin-dev-*", errStr)
			if err != nil {
				panic(err)
			}

			//var content Content
			//docs := result.Each(reflect.TypeOf(content))

			if result.Hits.TotalHits.Value > 0 {
				myErr := MyError{
					KeyWork: errStr,
					ID:      result.Hits.Hits[0].Id,
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
