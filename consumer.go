package rabbitmq

import (
	"fmt"
)

func (r *RabbitMQ) Consume(queueName string, handler func(msg string) error) error {
	ch, err := r.Conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open channel: %w", err)
	}
	defer ch.Close()

	_, err = ch.QueueDeclare(
		queueName,
		true, false, false, false, nil,
	)
	if err != nil {
		return fmt.Errorf("failed to declare queue %s: %w", queueName, err)
	}

	msgs, err := ch.Consume(
		queueName,
		"", true, false, false, false, nil,
	)
	if err != nil {
		return fmt.Errorf("failed to register consumer for %s: %w", queueName, err)
	}

	go func() {
		for d := range msgs {
			if err := handler(string(d.Body)); err != nil {
				fmt.Printf("Error handling message: %v\n", err)
			}
		}
	}()

	return nil
}
