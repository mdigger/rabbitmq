package rabbitmq_test

import (
	"context"
	"fmt"
	"time"

	"github.com/mdigger/rabbitmq"
	"github.com/rabbitmq/amqp091-go"
)

func Example() {
	// инициализируем контекст для выполнения
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute/10)
	defer cancel()

	// объявляем очередь
	queue := rabbitmq.NewQueue("test.queue")
	// инициализируем для этой очереди обработчик входящих сообщений и получаем worker
	consumerWorker := queue.Consume(func(msg amqp091.Delivery) {
		fmt.Println("->", msg.MessageId)
	})
	// инициализируем функцию публикации новых сообщений и соответствующий ей обработчик
	pubFunc, pubWorker := rabbitmq.Publish(
		rabbitmq.WithTTL(time.Minute),
		rabbitmq.WithTimestamp())

	// подключаемся к серверу и запускаем работу наших обработчиков
	err := rabbitmq.Init(ctx, "amqp://guest:guest@localhost:5672/", consumerWorker, pubWorker)
	if err != nil {
		panic(err)
	}

	// публикуем новые сообщения в очередь с небольшой задержкой
	for i := 1; i <= 3; i++ {
		msg := amqp091.Publishing{
			MessageId:   fmt.Sprintf("msg.%02d", i),
			ContentType: "text/plain",
			Body:        []byte("data"),
		}
		// отправляем сообщения в нашу очередь
		err := pubFunc(ctx, "", queue.String(), msg)
		if err != nil {
			panic(err)
		}
		fmt.Println("<-", msg.MessageId)
		time.Sleep(time.Second)
	}

	<-ctx.Done()
	time.Sleep(time.Second)

	// Output:
	// <- msg.01
	// -> msg.01
	// <- msg.02
	// -> msg.02
	// <- msg.03
	// -> msg.03
}

func ExampleWork() {
	// инициализируем контекст для выполнения
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute/11)
	defer cancel()

	const queueName = "test.queue" // название очереди с сообщениями
	// подключаемся к серверу и запускаем автоматическую обработку входящих сообщений
	pubFunc, err := rabbitmq.Work(ctx,
		"amqp://guest:guest@localhost:5672/", // адрес сервера для подключения
		rabbitmq.NewQueue(queueName),         // очередь с входящими сообщениями
		func(msg amqp091.Delivery) { // обработчик входящих сообщений
			fmt.Println("->", msg.MessageId)
		})
	if err != nil {
		panic(err)
	}

	// публикуем сообщения
	for i := 1; i <= 3; i++ {
		time.Sleep(time.Second)
		msg := amqp091.Publishing{
			MessageId:   fmt.Sprintf("msg.%02d", i),
			ContentType: "text/plain",
			Body:        []byte("data"),
		}
		err := pubFunc(ctx, "", queueName, msg) // вызываем функцию публикации
		if err != nil {
			panic(err)
		}
		fmt.Println("<-", msg.MessageId)
	}

	<-ctx.Done()
	time.Sleep(time.Second)

	// Output:
	// <- msg.01
	// -> msg.01
	// <- msg.02
	// -> msg.02
	// <- msg.03
	// -> msg.03
}
