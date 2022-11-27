package rabbitmq

import (
	"github.com/rabbitmq/amqp091-go"
)

// Handler является синонимом для функции обработки входящих сообщений.
type Handler = func(amqp091.Delivery)

// Consume возвращает инициализированный обработчик входящих сообщений для указанной очереди.
//
// По умолчанию включено автоматическое подтверждение приёма сообщения.
// Для его отключения используйте опцию WithNoAutoAck().
func Consume(queue *Queue, handler Handler, opts ...ConsumeOption) Initializer {
	log := log.With().Stringer("queue", queue).Logger()
	log.Debug().Msg("init consumer")

	// функция инициализации соединения
	initializer := func(ch *amqp091.Channel) error {
		// инициализируем настройки для очереди
		if err := queue.Declare(ch, false); err != nil {
			return err
		}
		// инициализируем получение сообщений
		consumer, err := queue.Consume(ch, opts...)
		if err != nil {
			return err
		}

		// запускаем отдельный поток для обработки входящих сообщений
		go func() {
			for msg := range consumer {
				handler(msg)
			}
			log.Debug().Msg("consumer worker closed")
		}()

		return nil
	}

	return initializer
}

// qos описывает параметры ограничения получения сообщений.
type qos struct {
	count uint
	size  uint
}

func (qos *qos) Qos(ch *amqp091.Channel) error {
	if qos == nil || (qos.count == 0 && qos.size == 0) {
		return nil // ничего не делаем, если не задано
	}

	err := ch.Qos(int(qos.count), int(qos.size), false)
	log.Debug().Err(err).
		Str("module", "rabbitmq").
		Uint("count", qos.count).
		Uint("size", qos.size).
		Bool("global", false).
		Msg("queue qos")

	return err
}

// consumeOptions описывает поддерживаемые параметры для инициализации обработки сообщений.
type consumeOptions struct {
	name      string // название
	noAutoAck bool   // не подтверждать автоматически приём
	exclusive bool   // единоличный доступ
	noLocal   bool
	noWait    bool
	args      amqp091.Table // дополнительные параметры
	*qos                    // ограничения по получению сообщений
}

// getOptions возвращает настройки после применения всех изменений.
func getConsumeOptions(opts []ConsumeOption) consumeOptions {
	var options consumeOptions
	for _, opt := range opts {
		opt.apply(&options)
	}
	return options
}

// ConsumeOption изменяет настройки получения сообщений.
type ConsumeOption interface{ apply(*consumeOptions) }

type consumeOptionFunc func(*consumeOptions)

func (f consumeOptionFunc) apply(o *consumeOptions) { f(o) }

// WithName задаёт имя обработчика сообщений.
func WithName(v string) ConsumeOption {
	return consumeOptionFunc(func(c *consumeOptions) { c.name = v })
}

// WithNoAutoAck запрещает автоматическое подтверждение приёма сообщений.
func WithNoAutoAck() ConsumeOption {
	return consumeOptionFunc(func(c *consumeOptions) { c.noAutoAck = true })
}

// WithExclusive взводит флаг эксклюзивного доступа к очереди.
func WithExclusive() ConsumeOption {
	return consumeOptionFunc(func(c *consumeOptions) { c.exclusive = true })
}

func WithNoLocal() ConsumeOption {
	return consumeOptionFunc(func(c *consumeOptions) { c.noLocal = true })
}

func WithNoWait() ConsumeOption {
	return consumeOptionFunc(func(c *consumeOptions) { c.noWait = true })
}

// WithArgs задает дополнительные параметры обработчика сообщений.
func WithArgs(v amqp091.Table) ConsumeOption {
	return consumeOptionFunc(func(c *consumeOptions) { c.args = v })
}

// WithQOS задаёт ограничение по получению сообщений.
func WithQOS(count, size uint) ConsumeOption {
	return consumeOptionFunc(func(c *consumeOptions) { c.qos = &qos{count: count, size: size} })
}
