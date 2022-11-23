package rabbitmq

import "github.com/rabbitmq/amqp091-go"

// ConsumeOptions описывает поддерживаемые параметры для инициализации обработки сообщений.
type ConsumeOptions struct {
	Name      string // название
	NoAutoAck bool   // не подтверждать автоматически приём
	Exclusive bool   // единоличный доступ
	NoLocal   bool
	NoWait    bool
	Args      amqp091.Table // дополнительные параметры
}

// getOptions возвращает настройки после применения всех изменений.
func getConsumeOptions(opts []ConsumeOption) ConsumeOptions {
	var options ConsumeOptions
	for _, opt := range opts {
		opt(&options)
	}
	return options
}

// ConsumeOption описывает интерфейс функции для задания необязательных параметров при настройке обработчика
// входящих сообщений.
type ConsumeOption func(*ConsumeOptions)

// WithName задаёт имя обработчика сообщений.
func WithName(v string) ConsumeOption {
	return func(c *ConsumeOptions) {
		c.Name = v
	}
}

// WithNoAutoAck запрещает автоматическое подтверждение приёма сообщений.
func WithNoAutoAck(v bool) ConsumeOption {
	return func(c *ConsumeOptions) {
		c.NoAutoAck = v
	}
}

// WithExclusive взводит флаг эксклюзивного доступа к очереди.
func WithExclusive(v bool) ConsumeOption {
	return func(c *ConsumeOptions) {
		c.Exclusive = v
	}
}

func WithNoLocal(v bool) ConsumeOption {
	return func(c *ConsumeOptions) {
		c.NoLocal = v
	}
}

func WithNoWait(v bool) ConsumeOption {
	return func(c *ConsumeOptions) {
		c.NoWait = v
	}
}

// WithArgs задает дополнительные параметры обработчика сообщений.
func WithArgs(v amqp091.Table) ConsumeOption {
	return func(c *ConsumeOptions) {
		c.Args = v
	}
}

type PublishOptions struct {
	Mandatory bool
	Immediate bool
}

type PublishOption func(*PublishOptions)

func WithMandatory(v bool) PublishOption {
	return func(c *PublishOptions) {
		c.Mandatory = v
	}
}

func WithImmediate(v bool) PublishOption {
	return func(c *PublishOptions) {
		c.Immediate = v
	}
}

// getOptions возвращает настройки после применения всех изменений.
func getPublishOpts(opts []PublishOption) PublishOptions {
	var options PublishOptions
	for _, opt := range opts {
		opt(&options)
	}
	return options
}
