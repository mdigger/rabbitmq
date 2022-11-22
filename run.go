package rabbitmq

import (
	"context"
	"fmt"

	"github.com/rabbitmq/amqp091-go"
	"golang.org/x/sync/errgroup"
)

// RabbitWorker описывает функцию для работы с соединением RabbitMQ.
type RabbitWorker = func(*amqp091.Channel) error

// Init запускает асинхронное выполнение процессов работы с RabbitMQ.
// Ожидает начального завершения процесс инициализации и только после этого возвращает управление.
// Внутри вызывает Run.
func Init(ctx context.Context, addr string, workers ...RabbitWorker) (err error) {
	stop := make(chan struct{}) // канал для отслеживания инициализации
	go func() {
		defer close(stop) // по окончании или ошибке тоже закрываем, если не дошло до нашего сервиса
		// добавляем свой обработчик, чтобы отследить окончание процесса инициализации
		err = Run(ctx, addr, append(workers, func(*amqp091.Channel) error {
			close(stop) // закрываем канал при инициализации сервиса
			return nil  // завершаем работу сервиса без ошибки
		})...) // запускаем параллельную обработку
	}()
	<-stop // ожидаем завершения инициализации
	return
}

// Run осуществляет подключение к серверу RabbitMQ и инициализирует обработчики сервисов с этим соединением.
// В случае сбоя пытается восстановить соединение и заново инициализировать обработчики.
//
// Для каждого обработчика инициализируется отдельный канал в рамках общего соединения с RabbitMQ. Этот канал
// передается обработчику, чтобы он мог проинициализировать работу с сервером и выполнять свои действия.
//
// Плановое завершение работы сервисов осуществляется через контекст. Так же происходит остановка всех сервисов,
// если хотя бы один из обработчиков возвращает ошибку.
func Run(ctx context.Context, addr string, workers ...RabbitWorker) error {
	// повторяем подключение в случае сбоя
	for {
		conn, err := Connect(addr) // подключаемся к серверу
		if err != nil {
			return fmt.Errorf("connection error: %w", err)
		}

		// инициализируем группу обработчиков соединения
		g, gCtx := errgroup.WithContext(ctx)

		// добавляем обработчик, который отслеживает ошибки соединения
		g.Go(func() error {
			select {
			case err = <-conn.NotifyClose(make(chan *amqp091.Error)):
				return fmt.Errorf("connection close: %w", err)

			case <-gCtx.Done(): // плановое завершение
				err := gCtx.Err()
				return fmt.Errorf("connection done: %w", err)
			}
		})

		// запускаем зарегистрированные для данного соединения обработчики
		for _, worker := range workers {
			// для каждого сервиса создаём отдельный канал
			ch, err := conn.Channel()
			if err != nil {
				conn.Close() // закрываем соединение при ошибке
				return fmt.Errorf("channel creation error: %w", err)
			}

			worker := worker // копируем в текущий стек
			// запускаем обработку сервиса в отдельном потоке
			g.Go(func() error {
				defer ch.Close()  // закрываем канал по завершению
				err := worker(ch) // запускаем обработчик сервиса на заданном канале
				if err != nil {
					return fmt.Errorf("service error: %w", err)
				}
				return nil
			})
		}

		err = g.Wait()        // ожидаем завершения всех обработчиков сервисов
		conn.Close()          // закрываем по окончании
		if ctx.Err() != nil { // отслеживаем плановую остановку сервиса
			return nil
		}
		// во всех остальных случаях осуществляем повторное соединение и инициализацию
	}
}
