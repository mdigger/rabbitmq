package rabbitmq

import (
	"time"

	"github.com/rabbitmq/amqp091-go"
)

// Параметры для переподключения к серверу RabbitMQ.
var (
	ReconnectDelay = time.Second * 2 // задержка перед повторным соединением
	MaxIteration   = 5               // максимальное количество попыток
)

// Connect возвращает инициализированное подключение к серверу RabbitMQ.
// В случае ошибки подключения попытка повторяется несколько раз (MaxIteration)
// с небольшой задержкой (ReconnectTime).
func Connect(addr string) (conn *amqp091.Connection, err error) {
	for i := 0; i < MaxIteration; i++ {
		conn, err = amqp091.Dial(addr) // подключаемся к серверу
		if err == nil {
			return conn, nil // в случае успешного подключения сразу возвращаем его
		}
		time.Sleep(ReconnectDelay) // задержка перед повтором попытки соединения
	}
	// все попытки подключения исчерпаны
	return nil, err
}
