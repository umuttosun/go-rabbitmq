package rabbitmq

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

func (r *RabbitMQ) Publish(queueName, body string) error {
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

	err = ch.Publish(
		"", queueName, false, false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		},
	)
	if err != nil {
		return fmt.Errorf("failed to publish message to %s: %w", queueName, err)
	}

	return nil
}
