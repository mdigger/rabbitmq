package rabbitmq

import (
	"strings"

	"github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"
)

// Convert преобразует сообщение из формата protobuf в сообщение rabbitMQ.
// Автоматически добавляет идентификатор исходного сообщения, чтобы можно было отследить ответ на него.
// В качестве типа сообщения устанавливает названия сообщения protobuf без названия пакета с его определением.
func Convert(id string, msg proto.Message) amqp091.Publishing {
	data, _ := proto.Marshal(msg)
	name := string(proto.MessageName(msg))
	// обрезаем название пакета из полного имени
	if idx := strings.LastIndexByte(name, '.'); idx > -1 {
		name = name[idx+1:]
	}
	return amqp091.Publishing{
		ContentType:   "application/protobuf",
		CorrelationId: id,
		Type:          name,
		Body:          data,
	}
}
