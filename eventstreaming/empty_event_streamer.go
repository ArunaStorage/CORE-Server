package eventstreaming

import v1 "github.com/ScienceObjectsDB/go-api/api/services/v1"

type emptyEventStreamMgmt struct {
}

type emptyEventStreamer struct {
}

func (mgmt *emptyEventStreamMgmt) EnableTestMode() error {
	return nil
}

func (mgmt *emptyEventStreamMgmt) CreateMessageStreamHandler(request *v1.NotificationStreamRequest) (EventStreamer, error) {
	return emptyEventStreamer{}, nil
}

func (mgmt *emptyEventStreamMgmt) PublishMessage(request *v1.EventNotificationMessage, resource v1.NotificationStreamRequest_EventResources) error {
	return nil
}

func (streamer emptyEventStreamer) GetResponseMessageChan() chan *v1.NotificationStreamResponse {
	return make(chan *v1.NotificationStreamResponse)
}

func (streamer emptyEventStreamer) StartMessageTransformation() error {
	return nil
}
