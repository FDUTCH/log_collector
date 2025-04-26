package writer

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"log/slog"
	"time"
)

type ElasticWriter struct {
	client    *elasticsearch.Client
	log       *slog.Logger
	topicName string
}

func NewElastic(log *slog.Logger, addresses ...string) (*ElasticWriter, error) {
	e := &ElasticWriter{log: log.With("src", "elastic"), topicName: "default"}
	err := e.connectElastic(addresses)
	return e, err
}

func (w *ElasticWriter) WithTopic(topic string) *ElasticWriter {
	el := *w
	el.log = w.log.With("topic", topic)
	el.topicName = topic
	return &el
}

func (w *ElasticWriter) Write(p []byte) (n int, err error) {
	n = len(p)
	data := map[string]any{}
	err = json.Unmarshal(p, &data)

	if err == nil {
		_, has := data["@timestamp"]
		if !has {
			data["@timestamp"] = time.Now().Format(time.RFC3339)
			p, _ = json.Marshal(data)
		}
	}

	resp, err := esapi.IndexRequest{
		Index:   w.index(),
		Body:    bytes.NewReader(p),
		Refresh: "false",
	}.Do(context.Background(), w.client)
	if err != nil {
		return 0, err
	}

	w.log.Debug("written", "data", string(p))

	_ = resp.Body.Close()
	return n, nil
}

func (w *ElasticWriter) index() string {
	return w.topicName + "-" + time.Now().Format(time.DateOnly)
}

func (w *ElasticWriter) connectElastic(addresses []string) error {
	w.log.Debug("connecting to db...")
	client, err := elasticsearch.NewClient(elasticsearch.Config{Addresses: addresses})
	if err != nil {
		w.log.Error("error connecting to db", "err", err)
		return err
	}

	w.client = client
	return nil
}
