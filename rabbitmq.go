package rabbitmq

import (
	"fmt"
	"log"
	"math"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitMQ struct {
	Conn *amqp.Connection
}

type RabbitMQConfig struct {
	Host     string
	Port     string
	Username string
	Password string
}

func ConnectRabbitMQ(config *RabbitMQConfig) (*RabbitMQ, error) {
	var counts int64
	var backOff = 1 * time.Second

	rabbitMQURI := fmt.Sprintf("amqp://%s:%s@%s:%s",
		config.Username, config.Password, config.Host, config.Port,
	)

	for {
		conn, err := amqp.Dial(rabbitMQURI)
		if err != nil {
			counts++
			if counts > 5 {
				return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
			}
			backOff = time.Duration(math.Pow(float64(counts), 2)) * time.Second
			log.Printf("RabbitMQ not ready, backing off for %v...\n", backOff)
			time.Sleep(backOff)
			continue
		}

		log.Println("Connected to RabbitMQ")
		return &RabbitMQ{Conn: conn}, nil
	}
}
