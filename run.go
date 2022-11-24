package rabbitmq

import (
	"context"
	"runtime"
	"sync"

	"github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
)

// ChannelHandler является синонимом функции для работы с соединением RabbitMQ.
// Используется в качестве обработчиков соединения.
type ChannelHandler = func(*amqp091.Channel) error

// Run осуществляет подключение к серверу RabbitMQ и инициализирует обработчики с этим соединением.
// Для каждого обработчика создаётся отдельный канал, а в случае ошибки инициализации всё повторяется.
//
// Возвращает ошибку, если превышено количество попыток установки соединений.
// Плановое завершение работы сервисов осуществляется через контекст.
func Run(ctx context.Context, addr string, workers ...ChannelHandler) error {
	log := log.With().Str("module", "rabbitmq").Logger()

	for {
		log.Debug().Msg("connecting...")
		conn, err := Connect(addr) // подключаемся к серверу
		if err != nil {
			return err // ошибка установки соединения
		}

		// инициализируем группу обработчиков соединения
		group, groupCtx := errgroup.WithContext(ctx)
		// добавляем обработчик, который отслеживает ошибки соединения
		group.Go(func() error {
			select {
			case err = <-conn.NotifyClose(make(chan *amqp091.Error)):
				return err // при плановом закрытии ошибка будет пустая
			case <-groupCtx.Done(): // плановое завершение
				return nil
			}
		})

		// запускаем зарегистрированные для данного соединения обработчики
		for _, worker := range workers {
			if groupCtx.Err() != nil {
				break // прерываем инициализацию при ошибке в одном из сервисов, соединении или глобальной остановке
			}

			worker := worker // копируем в текущий стек
			group.Go(func() error {
				ch, err := conn.Channel() // для каждого сервиса создаём отдельный канал
				if err != nil {
					return err
				}
				return worker(ch) // инициализируем обработчик сервиса на заданном канале
				// канал может использоваться и после закрытия обработчика, поэтому не закрываем его сами
			})
			runtime.Gosched() // позволить запуститься и отработать потоку
		}

		log.Debug().Msg("launched...")
		group.Wait()          // ожидаем завершения всех обработчиков сервисов
		conn.Close()          // закрываем соединение
		if ctx.Err() != nil { // отслеживаем плановую остановку сервиса
			log.Debug().Msg("stopped...")
			return nil
		}
		// осуществляем повторное соединение и инициализацию
	}
}

// Init запускает асинхронное выполнение Run и ожидает завершения самого первого процесса инициализации,
// после чего возвращает управление. Возвращает ошибку, если при первой инициализации обработчиков или установки
// соединения произошла ошибка.
func Init(ctx context.Context, addr string, workers ...ChannelHandler) error {
	var (
		stop       = make(chan struct{})    // канал для отслеживания инициализации
		end        = func() { close(stop) } // функция для закрытия канала
		once       sync.Once                // для однократного закрытия канала
		stopWorker = func(*amqp091.Channel) error {
			once.Do(end) // закрываем канал при инициализации сервиса
			return nil   // завершаем работу сервиса без ошибки
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

	<-stop // ожидаем завершения инициализации или её ошибки
	log.Debug().Str("module", "rabbitmq").Err(err).Msg("initialized")
	return err // возвращаем возможную ошибку первой инициализации
}

// Work является вспомогательной функцией быстрой инициализации для обработки входящих сообщений
// и публикации новых. В качестве параметров передаётся контекст для остановки сервиса, адрес для подключения
// к серверу RabbitMQ, очередь с входящими сообщениями и их обработчик. Кроме этого, можно указать необязательные
// параметры для публикации. Возвращает функцию для публикации новых сообщений.
//
// По умолчанию автоматически отсылается подтверждение о приёме входящих сообщений, а для исходящих заполняется
// поле `ReplyTo` указанием на очередь входящих сообщений.
func Work(ctx context.Context, addr string, queue *Queue, handler Handler, opts ...PublishOption) (Publisher, error) {
	consumerWorker := queue.Consume(handler)                       // обработка входящих сообщений
	opts = append([]PublishOption{WithReplyQueue(queue)}, opts...) // добавляем опцию публикации
	pubFunc, pubWorker := Publish(opts...)                         // публикация новых
	err := Init(ctx, addr, consumerWorker, pubWorker)              // запускаем подключение к серверу
	if err != nil {
		return nil, err
	}
	return pubFunc, nil // возвращаем функцию публикации
}
