package main

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
	"log"
	"net/http"
)

func main() {
	ctx := context.Background()

	cfg, err := GetConfig()
	if err != nil {
		log.Fatal("get config", err)
	}

	conn, err := pgxpool.New(ctx, cfg.Postgres)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	err = conn.Ping(ctx)
	if err != nil {
		log.Fatal(err)
	}
	connKafka, err := kafka.Dial("tcp", cfg.KafkaAddr)
	if err != nil {
		log.Fatal(err)
	}

	defer connKafka.Close()

	topicConfigs := []kafka.TopicConfig{{
		Topic:             cfg.KafkaTopic,
		NumPartitions:     1,
		ReplicationFactor: 1,
	},
	}
	err = connKafka.CreateTopics(topicConfigs...)
	if err != nil {
		log.Fatal(err)
	}

	kafkaWriter := &kafka.Writer{
		Addr:                   kafka.TCP(cfg.KafkaAddr),
		Topic:                  cfg.KafkaTopic,
		Balancer:               &kafka.LeastBytes{},
		AllowAutoTopicCreation: true,
	}

	defer kafkaWriter.Close()

	kafkaReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{cfg.KafkaAddr},
		Topic:     cfg.KafkaTopic,
		Partition: 0,
		MaxBytes:  10e6,
	})

	defer kafkaReader.Close()

	s := NewStorage(conn)
	h := NewHandler(s, kafkaWriter, kafkaReader)

	router := http.NewServeMux()
	router.HandleFunc("POST /messages", h.SaveMessages)
	router.HandleFunc("GET /statistic", h.ProcessedMessages)

	go h.SendEmail(ctx)
	go h.MessageProcessing()

	err = http.ListenAndServe(fmt.Sprintf(":%d", cfg.Port), router)
	if err != nil {
		log.Fatal(err)
	}
}
