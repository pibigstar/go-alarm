package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"reflect"
	"syscall"
	"time"

	"github.com/olivere/elastic"
	"github.com/robfig/cron"
)

const host = "http://127.0.0.1:9200"

type MyError struct {
	KeyWork string
	Content interface{}
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
	fmt.Printf("出现了: %s, 内容: %v, 发送至钉钉", err.KeyWork, err.Content)
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

type ConversationEsDoc struct {
	TID         int64     `json:"tid"`
	Content     string    `json:"content"`
	PublishedAt time.Time `json:"publishedAt"`
}

// search the result by query strings
func (client *esClient) Query(index, typeName string, queryStrings ...string) (*elastic.SearchResult, error) {
	var queryString string
	if len(queryStrings) > 0 {
		queryString = queryStrings[0]
	}
	// 根据名字查询
	query := elastic.NewQueryStringQuery(queryString)
	result, err := client.Search().Index(index).Type(typeName).TerminateAfter().Query(query).Do(client.ctx)
	if err != nil {
		return nil, err
	}
	if result.Hits.TotalHits > 0 {
		return result, nil
	}
	return nil, errors.New("query the result is null")
}

func main() {
	a := &AlarmError{}

	dingding := &DingDing{}
	a.alarms = append(a.alarms, dingding)

	c := cron.New()
	c.AddFunc("@every 1m", func() {
		a.findError("transactionError")
	})

	c.Start()

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, os.Kill, syscall.SIGTERM)
	<-interrupt
}

func (a *AlarmError) findError(errors ...string) {
	for _, err := range errors {
		go func(err string) {
			result, _ := client.Query("", "", err)
			if result != nil {
				docs := result.Each(reflect.TypeOf(ConversationEsDoc{}))
				for _, alarm := range a.alarms {
					myErr := MyError{
						KeyWork: err,
						Content: docs,
					}
					alarm.send(myErr)
				}
			}
		}(err)
	}
}

// Build the elasticSearch the client
func NewElasticSearchClient() error {
	errLog := log.New(os.Stdout, "Elastic", log.LstdFlags)

	esCli, err := elastic.NewClient(elastic.SetErrorLog(errLog), elastic.SetURL(host))
	if err != nil {
		return err
	}
	client = &esClient{Client: esCli, ctx: context.Background()}
	result, code, err := client.Ping(host).Do(context.Background())
	if err != nil {
		return err
	}
	log.Printf("Elasticsearch returned with code: %d and version: %s", code, result.Version.Number)

	version, err := client.ElasticsearchVersion(host)
	if err != nil {
		return err
	}
	log.Printf("Elasticsearch version :%s", version)

	return nil
}
