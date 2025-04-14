package rabbitmq

import (
	"fmt"
	"log"
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

// ConsumeFromExchange binds a queue to an exchange with a routing key and starts consuming
func (r *RabbitMQ) ConsumeFromExchange(exchangeName, exchangeType, queueName, routingKey string, handler func(msg string) error) error {
	ch, err := r.Conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open channel: %w", err)
	}
	defer ch.Close()

	// Declare the exchange
	err = ch.ExchangeDeclare(
		exchangeName,
		exchangeType,
		true,  // durable
		false, // auto-delete
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to declare exchange: %w", err)
	}

	// Declare the queue
	q, err := ch.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to declare queue: %w", err)
	}

	// Bind the queue to the exchange
	err = ch.QueueBind(
		q.Name,
		routingKey, // e.g., "info", "user.created"
		exchangeName,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to bind queue: %w", err)
	}

	// Start consuming
	msgs, err := ch.Consume(
		q.Name,
		"",
		true,  // auto-ack
		false, // exclusive
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to register consumer: %w", err)
	}

	log.Printf(" [*] Waiting for messages on %s (routing key: %s)\n", queueName, routingKey)

	go func() {
		for d := range msgs {
			if err := handler(string(d.Body)); err != nil {
				log.Printf("Handler error: %v\n", err)
			}
		}
	}()

	select {} // block forever
}
