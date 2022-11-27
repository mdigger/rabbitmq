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

// Declare декларирует очередь для канала соединения с RabbitMQ.
// В зависимости от параметра Passive, вызывается метод QueueDeclare или QueueDeclarePassive.
//
// Сохраняет возвращенное сервером название очереди, которое потом можно получить через метод String.
// Если возвращается ошибка, то декларация не прошла и канал после этого не действителен.
func (q *Queue) Declare(ch *amqp091.Channel, passive bool) error {
	// выбираем функцию для декларации, в зависимости от параметра Passive
	var declare func(name string, durable, autoDelete, exclusive, noWait bool, args amqp091.Table) (amqp091.Queue, error)
	if passive {
		declare = ch.QueueDeclarePassive
	} else {
		declare = ch.QueueDeclare
	}

	// декларируем описание очереди
	queue, err := declare(
		q.String(),   // name
		q.Durable,    // durable
		q.AutoDelete, // delete when unused
		q.Exclusive,  // exclusive
		q.NoWait,     // noWait
		q.Args,       // arguments
	)
	q.queue = queue.Name // сохраняем имя инициализированной очереди
	log.Debug().Err(err).
		Str("module", "rabbitmq").
		Str("queue", queue.Name).
		Msg("queue declare")

	return err
}

// Delete удаляет очередь, все привязки к ней и сообщения.
func (q *Queue) Delete(ch *amqp091.Channel, ifUnused, ifEmpty, noWait bool) error {
	_, err := ch.QueueDelete(q.String(), ifUnused, ifEmpty, noWait)
	log.Debug().Err(err).
		Str("module", "rabbitmq").
		Stringer("queue", q).
		Msg("queue delete")

	return err
}

// Bind связывает очередь с точкой обмена.
func (q *Queue) Bind(ch *amqp091.Channel, key, exchange string, noWait bool, args amqp091.Table) error {
	err := ch.QueueBind(q.String(), key, exchange, noWait, args)
	log.Debug().Err(err).
		Str("module", "rabbitmq").
		Stringer("queue", q).
		Msg("queue bind")

	return err
}

// UnBind отвязывает очередь от точкой обмена.
func (q *Queue) UnBind(ch *amqp091.Channel, key, exchange string, args amqp091.Table) error {
	err := ch.QueueUnbind(q.String(), key, exchange, args)
	log.Debug().Err(err).
		Str("module", "rabbitmq").
		Stringer("queue", q).
		Msg("queue unbind")

	return err
}

// Consume возвращает инициализированный канал с входящими сообщениями данной очереди.
func (q *Queue) Consume(ch *amqp091.Channel, opts ...ConsumeOption) (<-chan amqp091.Delivery, error) {
	options := getConsumeOptions(opts) // обобщаем параметры настройки

	// если заданы ограничения для получения сообщений, то применяем их
	if err := options.Qos(ch); err != nil {
		return nil, err
	}

	// инициализируем канал для получения входящих сообщений
	consumer, err := ch.Consume(
		q.String(),         // queue
		options.name,       // consumer
		!options.noAutoAck, // auto-ack
		options.exclusive,  // exclusive
		options.noLocal,    // no-local
		options.noWait,     // no-wait
		options.args,       // args
	)
	log.Debug().Err(err).
		Str("module", "rabbitmq").
		Stringer("name", q).
		Msg("queue consume")

	return consumer, err
}

// Inspect возвращает информацию об очереди.
func (q *Queue) Inspect(ch *amqp091.Channel) (amqp091.Queue, error) {
	return ch.QueueInspect(q.String())
}

// Purge удаляет все сообщения в очереди.
func (q *Queue) Purge(ch *amqp091.Channel, noWait bool) error {
	removed, err := ch.QueuePurge(q.String(), noWait)
	log.Debug().Err(err).
		Str("module", "rabbitmq").
		Stringer("name", q).
		Int("removed", removed).
		Msg("queue purge")

	return err
}
