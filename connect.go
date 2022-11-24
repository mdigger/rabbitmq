package rabbitmq

import (
	"net"
	"strconv"
	"time"

	"github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog/log"
)

// Параметры для переподключения к серверу RabbitMQ.
var (
	ReconnectDelay = time.Second * 2 // задержка перед повторным соединением
	MaxIteration   = 5               // максимальное количество попыток
)

// Connect возвращает инициализированное подключение к серверу RabbitMQ.
// В случае ошибки подключения попытка повторяется несколько раз (`MaxIteration`)
// с небольшой задержкой (`ReconnectTime`).
func Connect(addr string) (conn *amqp091.Connection, err error) {
	uri, _ := amqp091.ParseURI(addr) // разбираем адрес для вывода в лог
	addrStr := net.JoinHostPort(uri.Host, strconv.Itoa(uri.Port))
	log := log.With().
		Str("module", "rabbitmq").
		Str("addr", addrStr).
		Str("user", uri.Username).
		Logger()

	for i := 0; i < MaxIteration; i++ {
		conn, err = amqp091.Dial(addr) // подключаемся к серверу
		log.Err(err).Msg("connection")
		if err == nil {
			return conn, nil // в случае успешного подключения сразу возвращаем его
		}
		time.Sleep(ReconnectDelay) // задержка перед повтором попытки соединения
	}
	// все попытки подключения исчерпаны
	return nil, err
}
