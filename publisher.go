package rabbitmq

import (
	"context"
	"errors"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

// Publisher описывает функцию для публикации сообщений на сервер RabbitMQ.
type Publisher = func(ctx context.Context, exchange, key string, msg amqp091.Publishing) error

// ErrNoChannel описывает ошибку не инициализированного канала.
var ErrNoChannel = errors.New("channel is not initialized")

// Publish возвращает функцию и обработчик для публикации сообщений.
//
// Если перед публикацией необходимо произвести некоторые настройки канала, то можно задать свою функцию инициализации
// с помощью опции WithInit(ChannelHandler).
func Publish(opts ...PublishOption) (Publisher, Initializer) {
	log.Debug().Msg("init publisher")

	options := getPublishOpts(opts)       // суммарные опции для публикации
	var storedPublishingFunc atomic.Value // для ссылки на функцию публикации

	// функция инициализации подключения
	initializer := func(ch *amqp091.Channel) error {
		log.Debug().Msg("init publishing worker")

		// запускаем функцию инициализации сразу после установки соединения, если такая функция задана
		if options.init != nil {
			if err := options.init(ch); err != nil {
				log.Err(err).Msg("publishing initialization")
				return err
			}
		}

		// инициализируем функцию для публикации в канал с учётом всех опций
		publishingFunc := func(ctx context.Context, exchange, key string, msg amqp091.Publishing) error {
			return ch.PublishWithContext(ctx, exchange, key, options.mandatory, options.immediate, msg)
		}
		// сохраняем функцию для дальнейшего использования
		storedPublishingFunc.Store(Publisher(publishingFunc))

		return nil // больше ничего делать не нужно
	}

	// функция для публикации новых сообщений
	publisher := func(ctx context.Context, exchange, key string, msg amqp091.Publishing) error {
		log := log.Debug().Str("key", key)
		if exchange != "" {
			log = log.Str("exchange", exchange)
		}
		if msg.MessageId != "" {
			log = log.Str("messageId", msg.MessageId)
		}
		log.Msg("publishing")

		publishingFunc := storedPublishingFunc.Load() // получаем функцию для публикации
		if publishingFunc == nil {
			return ErrNoChannel // функция не инициализирована
		}

		// заполняем поле с названием очереди для ответа, если она задана
		if msg.ReplyTo == "" {
			if options.replyToQueue != nil {
				msg.ReplyTo = options.replyToQueue.String()
			} else {
				msg.ReplyTo = options.replyTo
			}
		}

		// добавляем временную метку, если это задано настройками
		if msg.Timestamp.IsZero() && options.timestamp {
			msg.Timestamp = time.Now()
		}

		// добавляем время жизни сообщения, если это задано
		if msg.Expiration == "" && options.ttl > 0 {
			msg.Expiration = strconv.FormatInt(time.Now().Add(options.ttl).Unix(), 10)
		}

		// задаём идентификатор приложения
		if options.appID != "" {
			msg.AppId = options.appID
		}

		return publishingFunc.(Publisher)(ctx, exchange, key, msg) // публикуем
	}

	return publisher, initializer
}

// publishOptions описывает дополнительный параметры публикации.
type publishOptions struct {
	mandatory    bool
	immediate    bool
	timestamp    bool          // добавлять время в сообщение
	init         Initializer   // функция инициализации
	appID        string        // идентификатор приложения
	replyToQueue *Queue        // очередь для ответа
	replyTo      string        // название очереди для ответа
	expiration   string        // время жизни сообщения
	ttl          time.Duration // время жизни сообщения
}

type PublishOption func(*publishOptions)

func WithMandatory() PublishOption {
	return func(c *publishOptions) {
		c.mandatory = true
	}
}

func WithImmediate() PublishOption {
	return func(c *publishOptions) {
		c.immediate = true
	}
}

// WithAppID задаёт идентификатор приложения, добавляемый во все отправляемые сообщения,
// перезаписывая любые ранее заданные в сообщении значения.
func WithAppID(v string) PublishOption {
	return func(c *publishOptions) {
		c.appID = v
	}
}

// WithReplyTo автоматически заполняет во всех отправляемых сообщениях поле ReplyTo заданным значением,
// если оно не заполнено в сообщении.
func WithReplyTo(v string) PublishOption {
	return func(c *publishOptions) {
		c.replyTo = v
	}
}

// WithReplyQueue заполняет поле ReplyTo во всех сообщениях именем указанной очереди.
// Если имя очереди меняется, то для всех новых сообщений так же будет использовано новое имя.
//
// При одновременном использовании с WithReplyTo, очередь имеет больший приоритет и будет
// использоваться именно она.
func WithReplyQueue(v *Queue) PublishOption {
	return func(c *publishOptions) {
		c.replyToQueue = v
	}
}

// WithTimestamp добавляет временную метку перед отправкой сообщения, если она не задана.
func WithTimestamp() PublishOption {
	return func(c *publishOptions) {
		c.timestamp = true
	}
}

// WithInit задаёт функцию для инициализации канала при подключении.
func WithInit(v Initializer) PublishOption {
	return func(c *publishOptions) {
		c.init = v
	}
}

// WithTTL задаёт ограничение по времени жизни сообщения.
func WithTTL(v time.Duration) PublishOption {
	return func(c *publishOptions) {
		c.ttl = v
	}
}

// getOptions возвращает настройки после применения всех изменений.
func getPublishOpts(opts []PublishOption) publishOptions {
	var options publishOptions
	for _, opt := range opts {
		opt(&options)
	}
	return options
}
