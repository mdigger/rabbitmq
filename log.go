package rabbitmq

import (
	"github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog"
)

// log используется как лог для библиотеки
var log = zerolog.Nop()

// SetLogger настраивает публикацию логов работы.
// Не является потокобезопасным методом и рекомендуется переопределять перед началом работы с библиотекой.
func SetLogger(l zerolog.Logger) {
	log = l                 // устанавливаем лог по умолчанию
	amqp091.SetLogger(&log) // задаём лог для самой библиотеки amqp091-go
}
