package rabbitmq

import (
	"context"
	"sync"

	"github.com/rabbitmq/amqp091-go"
)

// Initializer является синонимом функции для инициализации канала соединения RabbitMQ.
type Initializer = func(*amqp091.Channel) error

// Run осуществляет подключение к серверу RabbitMQ и инициализирует обработчики с этим соединением.
// Для каждого обработчика создаётся отдельный канал, а в случае ошибки инициализации всё повторяется.
//
// Возвращает ошибку, если превышено количество попыток установки соединений.
// Плановое завершение осуществляется через контекст.
func Run(ctx context.Context, addr string, initializers ...Initializer) error {
	for {
		conn, err := Connect(addr) // подключаемся к серверу
		if err != nil {
			return err // ошибка установки соединения
		}

		// запускаем зарегистрированные для данного соединения обработчики
		for _, init := range initializers {
			var ch *amqp091.Channel
			ch, err = conn.Channel() // для каждого сервиса создаём отдельный канал
			if err != nil {
				break
			}
			// инициализируем обработчик сервиса на заданном канале
			if err = init(ch); err != nil {
				ch.Close()
				break
			}
		}

		log.Debug().Err(err).Msg("initialized")
		// ожидаем закрытия соединения или сигнала об остановке
		if err == nil {
			select {
			case err = <-conn.NotifyClose(make(chan *amqp091.Error)):
				log.Err(err).Msg("connection closed")
			case <-ctx.Done(): // плановое завершение
			}
		}

		conn.Close()                      // закрываем соединение
		if err := ctx.Err(); err != nil { // отслеживаем плановую остановку сервиса
			log.Debug().Str("reason", err.Error()).Msg("stopped")
			return nil
		}
		// осуществляем повторное соединение и инициализацию
	}
}

// Init запускает асинхронное выполнение Run и ожидает завершения самого первого процесса инициализации,
// после чего возвращает управление. Возвращает ошибку, если при первой инициализации обработчиков или установке
// соединения произошла ошибка.
func Init(ctx context.Context, addr string, workers ...Initializer) error {
	var (
		stop       = make(chan struct{})    // канал для отслеживания инициализации
		end        = func() { close(stop) } // функция для закрытия канала
		once       sync.Once                // для однократного закрытия канала
		stopWorker = func(ch *amqp091.Channel) error {
			defer once.Do(end) // закрываем наш канал для сигнализации о выполнении
			return ch.Close()  // этот канал rabbitMQ больше не нужен
		}
		err error // отслеживаем ошибку первой инициализации сервисов при запуске
	)

	// запускаем параллельную работу обработчиков RabbitMQ соединения;
	// при ошибке или окончания первой инициализации всех обработчиков завершает свою работу, возвращая ошибку
	go func() {
		defer once.Do(end) // по окончании или ошибке тоже закрываем, если не дошло до нашего сервиса
		// добавляем свой обработчик в конец, чтобы отследить окончание процесса инициализации
		err = Run(ctx, addr, append(workers, stopWorker)...)
	}()

	<-stop     // ожидаем завершения инициализации или её ошибки
	return err // возвращаем возможную ошибку первой инициализации
}

// Work является вспомогательной функцией быстрой инициализации одновременной обработки входящих сообщений
// и публикации новых. В качестве параметров передаётся контекст для остановки сервиса, адрес для подключения
// к серверу RabbitMQ, очередь с входящими сообщениями и их обработчик. Кроме этого можно указать необязательные
// параметры для публикации. Возвращает функцию для публикации новых сообщений.
//
// По умолчанию автоматически отсылается подтверждение о приёме входящих сообщений, а для исходящих заполняется
// поле ReplyTo указанием на очередь входящих сообщений.
func Work(ctx context.Context, addr string, queue *Queue, handler Handler, opts ...PublishOption) (Publisher, error) {
	consumerWorker := Consume(queue, handler)                        // обработка входящих сообщений
	opts = append([]PublishOption{WithReplyToQueue(queue)}, opts...) // добавляем опцию публикации
	pubFunc, pubWorker := Publish(opts...)                           // публикация новых
	err := Init(ctx, addr, consumerWorker, pubWorker)                // запускаем подключение к серверу
	if err != nil {
		return nil, err
	}
	return pubFunc, nil // возвращаем функцию публикации
}
