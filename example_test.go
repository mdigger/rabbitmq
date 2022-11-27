package rabbitmq_test

import (
	"context"
	"fmt"
	"time"

	"github.com/mdigger/rabbitmq"
	"github.com/rabbitmq/amqp091-go"
)

const addr = "amqp://guest:guest@localhost:5672/" // адрес для подключения к серверу

var ctx = context.Background() // контекст выполнения работы

func Example() {
	// инициализируем контекст для выполнения
	ctx, cancel := context.WithTimeout(ctx, time.Minute/10)
	defer cancel()

	const queueName = "test.queue"          // название очереди с сообщениями
	queue := rabbitmq.NewQueue(queueName)   // создаём описание очереди
	handler := func(msg amqp091.Delivery) { // обработчик входящих сообщений
		fmt.Println("->", msg.MessageId)
	}

	// подключаемся к серверу и запускаем автоматическую обработку входящих сообщений
	pubFunc, err := rabbitmq.Work(ctx, addr, queue, handler)
	if err != nil {
		panic(err)
	}

	// публикуем сообщения
	for i := 1; i <= 3; i++ {
		// формируем сообщение
		msg := amqp091.Publishing{
			MessageId:   fmt.Sprintf("msg.%02d", i),
			ContentType: "text/plain",
			Body:        []byte("data"),
		}
		// вызываем функцию публикации
		err := pubFunc(ctx, "", queueName, msg)
		if err != nil {
			panic(err)
		}
		fmt.Println("<-", msg.MessageId)
	}

	<-ctx.Done()

	// Output:
	// <- msg.01
	// <- msg.02
	// <- msg.03
	// -> msg.01
	// -> msg.02
	// -> msg.03
}

func ExampleConsume() {
	// функция для обработки входящих сообщений
	handler := func(msg amqp091.Delivery) {
		fmt.Println("->", msg.MessageId)
		msg.Ack(false) // подтверждаем обработку сообщения
	}
	// создаём описание очереди
	queue := rabbitmq.NewQueue("test.queue")
	// инициализируем для этой очереди обработчик входящих сообщений и получаем worker
	consumerWorker := rabbitmq.Consume(queue, handler, rabbitmq.WithNoAutoAck())
	// подключаемся к серверу и запускаем работу наших обработчиков
	err := rabbitmq.Init(ctx, addr, consumerWorker)
	if err != nil {
		panic(err)
	}
}

func ExamplePublish() {
	// инициализируем функцию публикации новых сообщений и соответствующий ей обработчик
	pubFunc, pubWorker := rabbitmq.Publish(
		rabbitmq.WithTTL(time.Minute),
		rabbitmq.WithTimestamp())

	// подключаемся к серверу и запускаем работу наших обработчиков
	err := rabbitmq.Init(ctx, addr, pubWorker)
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
		err := pubFunc(ctx, "", "test.queue", msg)
		if err != nil {
			panic(err)
		}
	}
}

func ExampleWork() {
	const queueName = "test.queue"          // название очереди с сообщениями
	queue := rabbitmq.NewQueue(queueName)   // создаём описание очереди
	handler := func(msg amqp091.Delivery) { // обработчик входящих сообщений
		fmt.Println("->", msg.MessageId)
	}

	// подключаемся к серверу и запускаем автоматическую обработку входящих сообщений
	pubFunc, err := rabbitmq.Work(ctx, addr, queue, handler)
	if err != nil {
		panic(err)
	}

	// формируем сообщение для отправки
	msg := amqp091.Publishing{
		MessageId:   "msg.test",
		ContentType: "text/plain",
		Body:        []byte("data"),
	}
	// вызываем функцию публикации
	err = pubFunc(ctx, "", queueName, msg)
	if err != nil {
		panic(err)
	}
}
