package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/segmentio/kafka-go"
	"log/slog"
	"net/http"
	"time"
)

type Status string

const (
	NotProcessed   Status = "NotProcessed"
	BeingProcessed Status = "BeingProcessed"
	Processed      Status = "Processed"
)

type Handler struct {
	storage *Storage
	writer  *kafka.Writer
	reader  *kafka.Reader
}

func NewHandler(st *Storage, wr *kafka.Writer, r *kafka.Reader) *Handler {
	return &Handler{
		storage: st,
		writer:  wr,
		reader:  r,
	}
}

type Message struct {
	ID        int64  `json:"id"`
	Value     string `json:"value"`
	Status    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func (h *Handler) SaveMessages(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var message Message

	err := json.NewDecoder(r.Body).Decode(&message)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}
	message.Status = NotProcessed
	message.CreatedAt = time.Now()
	message.UpdatedAt = message.CreatedAt

	id, err := h.storage.SaveMessage(ctx, message)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	message, err = h.storage.Message(ctx, id)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}
}
func (h *Handler) SendEmail(ctx context.Context) {
	for {
		messages, err := h.storage.MessagesNotProcessed(ctx)
		if err != nil {
			slog.Error("get messages not processed", "err", err)
			continue
		}

		for _, email := range messages {
			msg, err := json.Marshal(&email)
			if err != nil {
				slog.Error("marshal email", "err", err)
			}

			err = h.writer.WriteMessages(ctx, kafka.Message{Value: msg})
			if err != nil {
				slog.Error("write messages", "err", err)
			}
		}
		time.After(1 * time.Minute)
	}
}

func (h *Handler) MessageProcessing() {
	for {
		err := func() error {
			ctx := context.Background()
			msg, err := h.reader.ReadMessage(ctx)
			if err != nil {
				return fmt.Errorf("read message:%w", err)
			}

			var message Message
			err = json.Unmarshal(msg.Value, &message)
			if err != nil {
				return fmt.Errorf("unmarshal: %w", err)
			}

			message.UpdatedAt = time.Now()
			message.Status = Processed
			err = h.storage.UpdateStatus(ctx, message)
			if err != nil {
				return fmt.Errorf("update status:%w", err)
			}
			return nil
		}()
		if err != nil {
			fmt.Errorf("message processing: %w", err)
			continue
		}
	}
}

func (h *Handler) ProcessedMessages(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	messages, err := h.storage.ProcessedMessages(ctx)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}
	err = json.NewEncoder(w).Encode(messages)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}
}
