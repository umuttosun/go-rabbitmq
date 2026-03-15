package rabbitmq

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// ConsumeWithDeadLetter is like Consume but with two key differences:
//
//  1. The queue is declared with x-dead-letter-exchange so that RabbitMQ routes
//     rejected messages to the DLX automatically.
//  2. Manual acknowledgment is used: a nil handler error → Ack; a non-nil error
//     → Nack(requeue=false), which triggers the DLX route.
//
// dlxName must match the exchange declared at the RabbitMQ broker level
// (e.g. via a definitions.json loaded at startup).
func (r *RabbitMQ) ConsumeWithDeadLetter(queueName, dlxName string, handler func(msg string) error) error {
	ch, err := r.Conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open channel: %w", err)
	}
	defer ch.Close()

	_, err = ch.QueueDeclare(
		queueName,
		true,  // durable
		false, // auto-delete
		false, // exclusive
		false, // no-wait
		amqp.Table{
			"x-dead-letter-exchange": dlxName,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to declare queue %s: %w", queueName, err)
	}

	// auto-ack = false so we can Nack failed messages to the DLX.
	msgs, err := ch.Consume(
		queueName,
		"",    // consumer tag (auto-generated)
		false, // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to register consumer for %s: %w", queueName, err)
	}

	for d := range msgs {
		if err := handler(string(d.Body)); err != nil {
			// Nack with requeue=false → RabbitMQ routes to the DLX.
			_ = d.Nack(false, false)
		} else {
			_ = d.Ack(false)
		}
	}

	return nil
}
