package main

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Storage struct {
	conn *pgxpool.Pool
}

func NewStorage(conn *pgxpool.Pool) *Storage {
	return &Storage{
		conn: conn}
}

var errNotFound = errors.New("not found")

func (s *Storage) SaveMessage(ctx context.Context, message Message) (int64, error) {
	query := "INSERT INTO messages ( value, status, created_at, updated_at) VALUES ($1, $2, $3, $4) RETURNING id"

	var id int64

	err := s.conn.QueryRow(ctx, query, message.Value, message.Status, message.CreatedAt, message.UpdatedAt).Scan(&id)
	if err != nil {
		return 0, err
	}
	return id, nil
}

func (s *Storage) Message(ctx context.Context, id int64) (Message, error) {
	query := "SELECT id, value, status, created_at, updated_at FROM messages WHERE id = $1"
	var message Message

	err := s.conn.QueryRow(ctx, query, id).Scan(&message.ID, &message.Value, &message.Status, &message.CreatedAt, &message.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Message{}, errNotFound
		}
		return Message{}, err
	}
	return message, nil
}

func (s *Storage) MessagesNotProcessed(ctx context.Context) ([]Message, error) {
	query := "SELECT id, value, status, created_at, updated_at FROM messages WHERE status = $1"

	rows, err := s.conn.Query(ctx, query, NotProcessed)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var messages []Message

	for rows.Next() {
		var message Message
		err = rows.Scan(&message.ID, &message.Value, &message.Status, &message.CreatedAt, &message.UpdatedAt)
		if err != nil {
			return nil, err
		}
		messages = append(messages, message)
	}
	return messages, nil
}

func (s *Storage) UpdateStatus(ctx context.Context, message Message) error {
	query := "UPDATE messages SET status = $1, updated_at= $2 WHERE id= $3"
	_, err := s.conn.Exec(ctx, query, message.Status, message.UpdatedAt, message.ID)
	if err != nil {
		return err
	}
	return nil
}

func (s *Storage) ProcessedMessages(ctx context.Context) ([]Message, error) {
	query := "SELECT id,value, status, created_at, updated_at FROM messages WHERE status = $1 "

	rows, err := s.conn.Query(ctx, query, Processed)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var messages []Message

	for rows.Next() {
		var message Message
		err = rows.Scan(&message.ID, &message.Value, &message.Status, &message.CreatedAt, &message.UpdatedAt)
		if err != nil {
			return nil, err
		}
		messages = append(messages, message)
	}
	return messages, nil
}
