package rabbitmq

import (
	"context"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Publish sends body to queueName using context.Background().
// Prefer PublishWithContext when a request-scoped context is available.
func (r *RabbitMQ) Publish(queueName, body string) error {
	return r.PublishWithContext(context.Background(), queueName, body)
}

// PublishWithContext sends body to queueName, honouring ctx for cancellation.
// The underlying amqp091 PublishWithContext call returns promptly when ctx
// is cancelled, so callers are not blocked by a slow or partitioned broker.
func (r *RabbitMQ) PublishWithContext(ctx context.Context, queueName, body string) error {
	ch, err := r.Conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open channel: %w", err)
	}
	defer ch.Close()

	_, err = ch.QueueDeclarePassive(
		queueName,
		true, false, false, false, nil,
	)
	if err != nil {
		return fmt.Errorf("failed to declare queue %s: %w", queueName, err)
	}

	err = ch.PublishWithContext(
		ctx,
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

// PublishWithExchange sends a message to a specific exchange with a routing key.
func (r *RabbitMQ) PublishWithExchange(exchangeName, exchangeType, routingKey, body string) error {
	return r.PublishWithExchangeContext(context.Background(), exchangeName, exchangeType, routingKey, body)
}

// PublishWithExchangeContext sends a message to a specific exchange, honouring ctx.
func (r *RabbitMQ) PublishWithExchangeContext(ctx context.Context, exchangeName, exchangeType, routingKey, body string) error {
	ch, err := r.Conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open channel: %w", err)
	}
	defer ch.Close()

	err = ch.ExchangeDeclare(
		exchangeName,
		exchangeType,
		true,  // durable
		false, // auto-deleted
		false, // internal
		false, // no-wait
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to declare exchange %s: %w", exchangeName, err)
	}

	err = ch.PublishWithContext(
		ctx,
		exchangeName, routingKey, false, false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		},
	)
	if err != nil {
		return fmt.Errorf("failed to publish to exchange %s: %w", exchangeName, err)
	}

	return nil
}
