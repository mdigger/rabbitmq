package rabbitmq

import (
	"github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog"
)

// log используется как лог для библиотеки
var log = zerolog.Nop()

// SetLogger задаёт лог библиотеки.
// Не является потокобезопасным методом.
// Рекомендуется переопределять перед началом работы с библиотекой.
func SetLogger(l zerolog.Logger) {
	log = l
	amqp091.SetLogger(logger{Logger: log})
}

type logger struct{ zerolog.Logger }

func (l logger) Printf(format string, v ...any) { l.Logger.Printf(format, v...) }
