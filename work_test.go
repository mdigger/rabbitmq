package rabbitmq

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const addr = "amqp://guest:guest@localhost:5672/"

func receiver(msg amqp091.Delivery) {
	fmt.Println("->", msg.MessageId)
	// msg.Ack(false)
}

func TestWorker(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	// инициализируем контекст для выполнения
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute/10)
	defer cancel()

	const queueName = "test.queue" // название очереди с сообщениями
	// подключаемся к серверу и запускаем автоматическую обработку входящих сообщений
	pubFunc, err := Work(ctx,
		"amqp://guest:guest@localhost:5672/", // адрес сервера для подключения
		NewQueue(queueName),                  // очередь с входящими сообщениями
		func(msg amqp091.Delivery) { // обработчик входящих сообщений
			fmt.Println("->", msg.MessageId)
		})
	require.NoError(err, "ошибка инициализации")

	// публикуем сообщения
	for i := 1; i <= 5; i++ {
		time.Sleep(time.Second)
		msg := amqp091.Publishing{
			MessageId:   fmt.Sprintf("msg.%02d", i),
			ContentType: "text/plain",
			Body:        []byte("data"),
		}
		err := pubFunc(ctx, "", queueName, msg) // вызываем функцию публикации
		assert.NoError(err, "ошибка публикации")
		fmt.Println("<-", msg.MessageId)
	}

	<-ctx.Done()
	time.Sleep(time.Second)
}
