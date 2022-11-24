# rabbitMQ

Библиотека rabbitmq для работы с сервером RabbitMQ является вспомогательной и построена поверх
[`github.com/rabbitmq/amqp091-go`](https://github.com/rabbitmq/amqp091-go/). В неё добавлено понятие обработчиков 
соединения (каналов): на их основе осуществляется автоматическое восстановление подключения к серверу и восстановление 
топологии.

Для установки соединения с сервером и запуска обработчиков используется метод `Run` (синхронный) или
`Init` (асинхронный), который позволяет задать несколько обработчиков `ChannelHandler`. Эти обработчики будут
вызываться при каждой установке соединения, чтобы восстановить топологию и заново проинициализировать работу
своих сервисов.

В библиотеки представлены два генератора таких обработчиков: `Consume` для обработки входящих сообщений и `Publish`
для публикации.

Для инициализации одновременной обработки входящих событий и публикации новых можно воспользоваться вспомогательной
функцией `Work`.

```golang
// инициализируем контекст для выполнения
ctx, cancel := context.WithTimeout(context.Background(), time.Minute/10)
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
```
