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
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(20))
	defer cancel()

	pub, publishingWorker := Publish() // публикация
	queue := NewQueue("test.lib01")    // очередь для получения ответов
	// соединяемся и запускаем обработчик
	err := Init(ctx, addr, queue.Consume(receiver), publishingWorker)
	require.NoError(err, "ошибка инициализации обработчиков")

	// публикуем сообщения
	for i := 0; i < 5; i++ {
		msg := amqp091.Publishing{
			MessageId: fmt.Sprintf("msg.%03d", i+1),
			Body:      []byte("data"),
		}
		err := pub(ctx, "", queue.String(), msg) // публикую в наш канал
		assert.NoError(err, "ошибка посылки сообщения")
		fmt.Println("<-", msg.MessageId)
		time.Sleep(time.Second)
	}
}
