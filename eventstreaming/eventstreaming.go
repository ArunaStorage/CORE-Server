package eventstreaming

import (
	"fmt"

	"github.com/ScienceObjectsDB/CORE-Server/config"
	"github.com/ScienceObjectsDB/CORE-Server/database"
	v1 "github.com/ScienceObjectsDB/go-api/api/services/v1"
	"github.com/spf13/viper"
)

func New(db *database.Read) (EventStreamMgmt, error) {
	eventStreamBackendConfString := viper.GetString(config.EVENTNOTIFICATION_BACKEND)

	var streamMgmt EventStreamMgmt
	var err error

	switch eventStreamBackendConfString {
	case "Empty":
		streamMgmt = &emptyEventStreamMgmt{}
	case "NATS":
		streamMgmt, err = newNatsEventStreamMgmt(db)
	default:
		err = fmt.Errorf("no valid eventstreaming config found in EventNotifications.Backend, please specify either NATS or Empty")
	}

	return streamMgmt, err
}

type EventStreamMgmt interface {
	CreateMessageStreamHandler(request *v1.NotificationStreamRequest) (EventStreamer, error)
	PublishMessage(request *v1.EventNotificationMessage, resource v1.NotificationStreamRequest_EventResources) error
	EnableTestMode() error
}

type EventStreamer interface {
	GetResponseMessageChan() chan *v1.NotificationStreamResponse
	StartMessageTransformation() error
}
