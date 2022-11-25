package rabbitmq

import (
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

// String возвращает имя очереди. Возвращаемое значение может отличаться от Name.
// Если очередь была с пустым именем и прошла декларацию, то возвращаемое название очереди сгенерировано сервером.
func (q *Queue) String() string {
	if q.queue != "" {
		return q.queue
	}

	return q.Name
}

// declare декларирует очередь для канала соединения с RabbitMQ.
//
// Сохраняет возвращенное сервером название очереди, которое потом можно получить через метод String.
// Если возвращается ошибка, то декларация не прошла и канал после этого не действителен.
func (q *Queue) declare(ch *amqp091.Channel) error {
	queue, err := ch.QueueDeclare(
		q.String(),   // name
		q.Durable,    // durable
		q.AutoDelete, // delete when unused
		q.Exclusive,  // exclusive
		q.NoWait,     // noWait
		q.Args,       // arguments
	)
	q.queue = queue.Name // сохраняем имя инициализированной очереди

	log.Debug().Str("module", "rabbitmq").Str("queue", queue.Name).Msg("queue declare")
	return err
}

// Consume возвращает инициализированный обработчик входящих сообщений данной очереди.
func (q *Queue) Consume(handler func(amqp091.Delivery), opts ...ConsumeOption) Initializer {
	return Consume(q, handler, opts...)
}
