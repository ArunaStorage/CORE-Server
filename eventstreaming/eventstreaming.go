package eventstreaming

import v1 "github.com/ScienceObjectsDB/go-api/api/services/v1"

type EventStreamMgmt interface {
	CreateMessageStreamHandler(request *v1.NotificationStreamRequest) (EventStreamer, error)
	PublishMessage(request *v1.EventNotificationMessage, resource v1.NotificationStreamRequest_EventResources) error
}

type EventStreamer interface {
	GetResponseMessageChan() chan *v1.NotificationStreamResponse
	StartMessageTransformation() error
}
