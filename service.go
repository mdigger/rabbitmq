package rabbitmq

import (
	"context"
	"errors"
	"sync"

	"github.com/rabbitmq/amqp091-go"
)

// Handler описывает функцию для обработки входящих сообщений.
type Handler = func(amqp091.Delivery)

// SendReceive описывает сервис для посылки и приемке ответов
type SendReceive struct {
	Queue   string  // имя внутренней очереди для получения ответов
	Handler Handler // функция для обработки ответов

	ch    *amqp091.Channel // подключение к серверу
	queue string           // внутреннее сохраненное название очереди
	mu    sync.RWMutex     // блокировка доступа
}

// NewSendReceive возвращает инициализированный приёмку/отправку сообщений на RabbitMQ.
func NewSendReceive(queue string, handler Handler) *SendReceive {
	return &SendReceive{
		Queue:   queue,
		Handler: handler,
	}
}

// Run декларирует объявление очереди и начинает обработку получения сообщений от сервера.
func (sr *SendReceive) Run(ch *amqp091.Channel) error {
	q, err := ch.QueueDeclare(
		sr.Queue, // name
		false,    // durable
		false,    // delete when unused
		true,     // exclusive
		false,    // noWait
		nil,      // arguments
	)
	if err != nil {
		return err
	}

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		return err
	}

	sr.mu.Lock()
	sr.queue = q.Name
	sr.ch = ch
	sr.mu.Unlock()

	for msg := range msgs {
		sr.Handler(msg) // вызываем обработчик входящего сообщения
	}

	return nil
}

// Send отсылает сообщение на сервер, используя указанный ключ маршрутизации.
// Если не задано поле ReplyTo сообщения, то используется полученное при инициализации канала
// название внутренней очереди.
func (sr *SendReceive) Send(ctx context.Context, routingKey string, msg amqp091.Publishing) error {
	sr.mu.RLock()
	defer sr.mu.RUnlock()

	// добавляем название внутренней очереди для ответа, если оно не задано
	if msg.ReplyTo == "" {
		msg.ReplyTo = sr.queue
	}

	if sr.ch == nil {
		return errors.New("channel is nil")
	}

	// отправляем сообщение на сервер
	return sr.ch.PublishWithContext(ctx, "", routingKey, false, false, msg)
}
