// Библиотека rabbitmq для работы с сервером RabbitMQ является вспомогательной и построена поверх
// github.com/rabbitmq/amqp091-go. В неё добавлено понятие инициализаторов каналов соединения: на их основе
// осуществляется автоматическое восстановление подключения к серверу и восстановление состояния.

// Для установки соединения с сервером и запуска обработчиков используется метод `Run` (синхронный) или
// `Init` (асинхронный), которые позволяют задать несколько инициализаторов `Initializer`. Эти обработчики будут
// вызываться при каждой установке соединения, чтобы восстановить топологию и заново проинициализировать работу
// своих сервисов.

// В библиотеки представлены два генератора таких инициализаторов: `Consume` для обработки входящих сообщений и
// `Publish` для публикации. Для инициализации одновременной обработки входящих событий и публикации новых можно
// воспользоваться вспомогательной функцией `Work`.
package rabbitmq
