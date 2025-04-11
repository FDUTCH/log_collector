package collector

import (
	"github.com/FDUTCH/log_collector/writer"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)
import "github.com/IBM/sarama"

func Run(log *slog.Logger) error {
	host := os.Getenv("BROKER_HOST") + ":" + os.Getenv("BROKER_PORT")

	worker, err := newConsumer(host)
	if err != nil {
		log.Error("error creating new worker", "err", err, "host", host)
		return err
	}
	defer worker.Close()

	w, err := writer.NewElastic(log)
	if err != nil {
		return err
	}

	for _, topic := range topics() {
		log.Info("consuming", "topic", topic)
		consumer, err := worker.ConsumePartition(topic, 0, sarama.OffsetOldest)
		if err != nil {
			log.Error("error consume partition", "err", err, "topic", topic)
			continue
		}

		defer consumer.Close()
		go listen(w.WithTopic(topic), consumer)
	}

	c := make(chan os.Signal, 2)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)

	<-c
	return nil
}

func listen(w *writer.ElasticWriter, consumer sarama.PartitionConsumer) {
	for msg := range consumer.Messages() {
		if msg.Timestamp.Before(time.Now().Add(-time.Second)) {
			continue
		}
		_, _ = w.Write(msg.Value)
	}
}

func newConsumer(broker string) (sarama.Consumer, error) {
	cfg := sarama.NewConfig()
	cfg.Consumer.Return.Errors = true

	return sarama.NewConsumer([]string{broker}, cfg)
}

func topics() []string {
	return strings.Split(os.Getenv("BROKER_TOPICS"), ",")
}
