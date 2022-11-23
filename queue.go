package rabbitmq

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/rabbitmq/amqp091-go"
)

// Queue описывает очередь сообщений.
type Queue struct {
	Name       string        // название очереди (пустое для приватной)
	Durable    bool          // сохранять сообщения при перезагрузке
	AutoDelete bool          // автоматическое удаление очереди при отключении
	Exclusive  bool          // эксклюзивный доступ для текущего соединения
	NoWait     bool          // не ждать подтверждения декларирования от сервера
	Args       amqp091.Table // дополнительные параметры
	queue      string        // название сгенерированной очереди
}

// NewQueue возвращает новое описание очереди с заданным именем.
func NewQueue(name string) *Queue {
	return &Queue{Name: name}
}

// String возвращает имя очереди. Возвращаемое значение может отличаться от Name: если очередь была с пустым именем и
// прошла декларацию, то возвращаемое название очереди сгенерировано сервером.
func (q *Queue) String() string {
	if q.queue != "" {
		return q.queue
	}

	return q.Name
}

// Declare декларирует очередь для канала соединения с RabbitMQ.
// Сохраняет возвращенное сервером название очереди, которое потом можно получить через метод String.
// Если возвращается ошибка, то декларация не прошла и канал после этого не действителен.
func (q *Queue) Declare(ch *amqp091.Channel) error {
	queue, err := ch.QueueDeclare(
		q.String(),   // name
		q.Durable,    // durable
		q.AutoDelete, // delete when unused
		q.Exclusive,  // exclusive
		q.NoWait,     // noWait
		q.Args,       // arguments
	)
	q.queue = queue.Name // сохраняем имя инициализированной очереди
	return err
}

// Consume возвращает инициализированный канал с входящими сообщениями.
// Вызывает Declare перед началом процесса.
func (q *Queue) Consume(ch *amqp091.Channel, opts ...ConsumeOption) (<-chan amqp091.Delivery, error) {
	if err := q.Declare(ch); err != nil {
		return nil, err
	}

	options := getConsumeOptions(opts)
	return ch.Consume(
		q.String(),         // queue
		options.Name,       // consumer
		!options.NoAutoAck, // auto-ack
		options.Exclusive,  // exclusive
		options.NoLocal,    // no-local
		options.NoWait,     // no-wait
		options.Args,       // args
	)
}

// Handler описывает функцию для обработки входящих сообщений.
type Handler = func(amqp091.Delivery)

// ConsumeWorkerWithHandler инициализированный Worker с заданным обработчиком сообщений.
func (q *Queue) ConsumeWorkerWithHandler(handler Handler, opts ...ConsumeOption) Worker {
	return func(ch *amqp091.Channel) error {
		msgs, err := q.Consume(ch, opts...)
		if err != nil {
			return err
		}

		for msg := range msgs {
			handler(msg)
		}

		return nil
	}
}

// ConsumeWorker возвращает инициализированный канал с входящими сообщениями и Worker,
// поддерживающий переподключение.
func (q *Queue) ConsumeWorker(opts ...ConsumeOption) (<-chan amqp091.Delivery, Worker) {
	delivery := make(chan amqp091.Delivery)
	handler := func(msg amqp091.Delivery) {
		delivery <- msg
	}
	worker := q.ConsumeWorkerWithHandler(handler, opts...)
	return delivery, worker
}

// Publisher описывает функцию для публикации новых сообщений в очередь.
type Publisher = func(ctx context.Context, msg amqp091.Publishing) error

// Publisher возвращает функцию для публикации сообщений в указанную очередь через открытый канал RabbitMQ.
// При инициализации вызывает Declare.
func (q *Queue) Publisher(ch *amqp091.Channel, opts ...PublishOption) (Publisher, error) {
	if err := q.Declare(ch); err != nil {
		return nil, err
	}

	// возвращаем функцию для публикации
	options := getPublishOpts(opts)
	return func(ctx context.Context, msg amqp091.Publishing) error {
		return ch.PublishWithContext(ctx, "", q.String(), options.Mandatory, options.Immediate, msg)
	}, nil
}

// ErrNoChannel описывает ошибку не инициализированного канала.
var ErrNoChannel = errors.New("channel is not initialized")

// PublishWorker возвращает функцию для публикации сообщений в очередь и обработчик для запуска процесса.
func (q *Queue) PublishWorker(opts ...PublishOption) (Publisher, Worker) {
	var channelPublisher atomic.Value // функция для публикации в канал

	// функция для инициализации публикации при установке соединения
	worker := func(ch *amqp091.Channel) error {
		// получаем функцию публикации в очередь для данного канала
		publish, err := q.Publisher(ch, opts...)
		if err != nil {
			return err
		}

		channelPublisher.Store(publish) // сохраняем её для внешнего использования
		return nil                      // завершаем работу
	}

	// функция для публикации сообщения в канал установленного соединения
	publisher := func(ctx context.Context, msg amqp091.Publishing) error {
		publish := channelPublisher.Load().(Publisher) // получаем функцию для публикации
		if publish == nil {
			return ErrNoChannel
		}

		return publish(ctx, msg) // публикуем сообщение
	}

	// возвращаем функцию для публикации и функцию для инициализации обработки в случае переподключения
	return publisher, worker
}
