package rabbitmq

import (
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Consume declares the queue and starts a blocking consumer loop.
// The handler is called for each delivery; a nil error acknowledges the
// message, a non-nil error nacks it with requeue=true so it is retried.
//
// The function blocks until the channel is closed (e.g. connection lost).
// Call it in a goroutine if you need non-blocking start:
//
//	go func() { log.Fatal(r.Consume(queue, handler)) }()
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

	// auto-ack = false so that a failed handler does not silently drop messages.
	msgs, err := ch.Consume(
		queueName,
		"",    // consumer tag (auto-generated)
		false, // auto-ack
		false, false, false, nil,
	)
	if err != nil {
		return fmt.Errorf("failed to register consumer for %s: %w", queueName, err)
	}

	for d := range msgs {
		if err := handler(string(d.Body)); err != nil {
			fmt.Printf("Error handling message: %v\n", err)
			// Nack with requeue=true so the broker re-delivers the message.
			// Configure a DLX at the broker level to avoid infinite retry loops
			// for poison messages (use ConsumeWithDeadLetter instead).
			_ = d.Nack(false, true)
		} else {
			_ = d.Ack(false)
		}
	}

	return nil
}

// ConsumeFromExchange binds a queue to an exchange with a routing key and
// starts a blocking consumer loop.
//
// The function blocks until the channel is closed. Call it in a goroutine if
// you need non-blocking start.
func (r *RabbitMQ) ConsumeFromExchange(exchangeName, exchangeType, queueName, routingKey string, handler func(msg string) error) error {
	ch, err := r.Conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open channel: %w", err)
	}
	defer ch.Close()

	if err = ch.ExchangeDeclare(
		exchangeName,
		exchangeType,
		true,  // durable
		false, // auto-delete
		false,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("failed to declare exchange: %w", err)
	}

	q, err := ch.QueueDeclare(
		queueName,
		true, false, false, false, nil,
	)
	if err != nil {
		return fmt.Errorf("failed to declare queue: %w", err)
	}

	if err = ch.QueueBind(
		q.Name,
		routingKey,
		exchangeName,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("failed to bind queue: %w", err)
	}

	// auto-ack = false so that a failed handler does not silently drop messages.
	msgs, err := ch.Consume(
		q.Name,
		"",    // consumer tag (auto-generated)
		false, // auto-ack
		false, false, false, nil,
	)
	if err != nil {
		return fmt.Errorf("failed to register consumer: %w", err)
	}

	log.Printf(" [*] Waiting for messages on %s (routing key: %s)\n", queueName, routingKey)

	for d := range msgs {
		if err := handler(string(d.Body)); err != nil {
			log.Printf("Handler error: %v\n", err)
			_ = d.Nack(false, true) // requeue on error
		} else {
			_ = d.Ack(false)
		}
	}

	return nil
}

// ConsumePassive is like Consume but uses QueueDeclarePassive — it expects the
// queue to already exist and returns an error if it does not.
// Useful when the queue is declared by another service with specific arguments
// (e.g. DLX) that this consumer must not override.
func (r *RabbitMQ) ConsumePassive(queueName string, handler func(msg string) error) error {
	ch, err := r.Conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open channel: %w", err)
	}
	defer ch.Close()

	if _, err = ch.QueueDeclarePassive(
		queueName,
		true, false, false, false, nil,
	); err != nil {
		return fmt.Errorf("queue %s does not exist: %w", queueName, err)
	}

	msgs, err := ch.Consume(
		queueName, "", false, false, false, false, nil,
	)
	if err != nil {
		return fmt.Errorf("failed to register consumer for %s: %w", queueName, err)
	}

	for d := range msgs {
		if err := handler(string(d.Body)); err != nil {
			_ = d.Nack(false, true)
		} else {
			_ = d.Ack(false)
		}
	}
	return nil
}

// ConsumeDeclare declares the queue with the provided arguments (e.g. DLX) and
// starts consuming. Use this when the caller needs full control over queue
// arguments without needing a separate dead-letter exchange consumer.
func (r *RabbitMQ) ConsumeDeclare(queueName string, args amqp.Table, handler func(msg string) error) error {
	ch, err := r.Conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open channel: %w", err)
	}
	defer ch.Close()

	if _, err = ch.QueueDeclare(
		queueName, true, false, false, false, args,
	); err != nil {
		return fmt.Errorf("failed to declare queue %s: %w", queueName, err)
	}

	msgs, err := ch.Consume(
		queueName, "", false, false, false, false, nil,
	)
	if err != nil {
		return fmt.Errorf("failed to register consumer for %s: %w", queueName, err)
	}

	for d := range msgs {
		if err := handler(string(d.Body)); err != nil {
			_ = d.Nack(false, false) // do not requeue — send to DLX if configured
		} else {
			_ = d.Ack(false)
		}
	}
	return nil
}
