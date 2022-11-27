package rabbitmq

import "github.com/rabbitmq/amqp091-go"

// Exchange описывает параметры точки обмена для декларации.
type Exchange struct {
	Name       string
	Kind       string        // типы: "direct", "fanout", "topic", "headers"
	Durable    bool          // сохраняется между перезагрузками
	AutoDelete bool          // автоматически удаляется при отключении
	Internal   bool          // без поддержки публикации; используется для внутренней топологии
	NoWait     bool          // не ждать подтверждения декларации от сервера
	Args       amqp091.Table // дополнительные параметры
}

// Возвращает новое описание точки обмена.
func NewExchange(name, kind string) *Exchange {
	return &Exchange{Name: name, Kind: kind}
}

// Возвращает новое описание точки обмена с поддержкой сохранения между перезагрузками (long-lived).
func NewExchangeDurable(name, kind string) *Exchange {
	return &Exchange{Name: name, Kind: kind, Durable: true}
}

// Возвращает новое описание точки обмена с поддержкой автоматического удаления при неиспользовании.
func NewExchangeAutoDelete(name, kind string) *Exchange {
	return &Exchange{Name: name, Kind: kind, AutoDelete: true}
}

// String возвращает название точки обмена.
func (ex *Exchange) String() string {
	return ex.Name
}

// Declare декларирует описание точки обмена в канал.
// В зависимости от флага Passive, вызывается метод ExchangeDeclare или ExchangeDeclarePassive.
//
// Если возвращается ошибка, то канал необходимо закрыть.
func (ex *Exchange) Declare(ch *amqp091.Channel, passive bool) error {
	// выбираем функцию для декларирования точки обмена в зависимости от параметра Passive
	var declare func(name, kind string, durable, autoDelete, internal, noWait bool, args amqp091.Table) error
	if passive {
		declare = ch.ExchangeDeclarePassive
	} else {
		declare = ch.ExchangeDeclare
	}

	// декларируем описание точки обмена
	err := declare(ex.Name, ex.Kind, ex.Durable, ex.AutoDelete, ex.Internal, ex.NoWait, ex.Args)
	log.Debug().Err(err).
		Str("module", "rabbitmq").
		Str("name", ex.Name).
		Str("kind", ex.Kind).
		Bool("passive", passive).
		Msg("exchange declare")

	return err
}

// Delete удаляет описание точки обмена.
// Так же удаляются и все привязки очередей к этой точке обмена.
func (ex *Exchange) Delete(ch *amqp091.Channel, ifUnused bool, noWait bool) error {
	err := ch.ExchangeDelete(ex.Name, ifUnused, noWait)
	log.Debug().Err(err).
		Str("module", "rabbitmq").
		Str("name", ex.Name).
		Str("kind", ex.Kind).
		Msg("exchange delete")

	return err
}

// Bind связывает эту точку обмена с другой для внутреннего обмена.
func (ex *Exchange) Bind(ch *amqp091.Channel, destination, key string, noWait bool, args amqp091.Table) error {
	err := ch.ExchangeBind(destination, key, ex.Name, noWait, args)
	log.Debug().Err(err).
		Str("module", "rabbitmq").
		Str("name", ex.Name).
		Str("destination", destination).
		Str("key", key).
		Msg("exchange bind")

	return err
}

// UnBind отвязывает эту точку обмена от другой для внутреннего обмена.
func (ex *Exchange) UnBind(ch *amqp091.Channel, destination, key string, noWait bool, args amqp091.Table) error {
	err := ch.ExchangeUnbind(destination, key, ex.Name, noWait, args)
	log.Debug().Err(err).
		Str("module", "rabbitmq").
		Str("name", ex.Name).
		Str("destination", destination).
		Str("key", key).
		Msg("exchange unbind")

	return err
}
