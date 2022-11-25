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

	options := getConsumeOptions(opts) // обобщаем параметры настройки
	// функция инициализации соединения
	initializer := func(ch *amqp091.Channel) error {
		// инициализируем настройки для очереди
		if err := queue.declare(ch); err != nil {
			return err
		}

		// инициализируем получение сообщений
		consumer, err := ch.Consume(
			queue.String(),     // queue
			options.name,       // consumer
			!options.noAutoAck, // auto-ack
			options.exclusive,  // exclusive
			options.noLocal,    // no-local
			options.noWait,     // no-wait
			options.args,       // args
		)
		log.Debug().Err(err).Msg("init consume worker")
		if err != nil {
			return err
		}

		go func() {
			// получаем сообщения и вызываем их обработчик
			for msg := range consumer {
				handler(msg)
			}
			log.Debug().Msg("consumer worker closed")
		}()

		return nil
	}

	return initializer
}

// consumeOptions описывает поддерживаемые параметры для инициализации обработки сообщений.
type consumeOptions struct {
	name      string // название
	noAutoAck bool   // не подтверждать автоматически приём
	exclusive bool   // единоличный доступ
	noLocal   bool
	noWait    bool
	args      amqp091.Table // дополнительные параметры
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

type funcConsumeOption struct{ f func(*consumeOptions) }

func (fco *funcConsumeOption) apply(co *consumeOptions) { fco.f(co) }

func newFuncConsumeOption(f func(*consumeOptions)) *funcConsumeOption {
	return &funcConsumeOption{f: f}
}

// WithName задаёт имя обработчика сообщений.
func WithName(v string) ConsumeOption {
	return newFuncConsumeOption(func(c *consumeOptions) { c.name = v })
}

// WithNoAutoAck запрещает автоматическое подтверждение приёма сообщений.
func WithNoAutoAck() ConsumeOption {
	return newFuncConsumeOption(func(c *consumeOptions) { c.noAutoAck = true })
}

// WithExclusive взводит флаг эксклюзивного доступа к очереди.
func WithExclusive() ConsumeOption {
	return newFuncConsumeOption(func(c *consumeOptions) { c.exclusive = true })
}

func WithNoLocal() ConsumeOption {
	return newFuncConsumeOption(func(c *consumeOptions) { c.noLocal = true })
}

func WithNoWait() ConsumeOption {
	return newFuncConsumeOption(func(c *consumeOptions) { c.noWait = true })
}

// WithArgs задает дополнительные параметры обработчика сообщений.
func WithArgs(v amqp091.Table) ConsumeOption {
	return newFuncConsumeOption(func(c *consumeOptions) { c.args = v })
}
