package eventstreaming

import (
	"fmt"

	"github.com/ScienceObjectsDB/CORE-Server/models"
	v1 "github.com/ScienceObjectsDB/go-api/api/services/v1"
	"github.com/google/uuid"
)

type emptyEventStreamMgmt struct {
}

type emptyEventStreamer struct {
}

func (mgmt *emptyEventStreamMgmt) EnableTestMode() error {
	return nil
}

func (mgmt *emptyEventStreamMgmt) CreateStreamGroup(projectID uuid.UUID, resourceID uuid.UUID, resourceType *v1.CreateEventStreamingGroupRequest_EventResources, includeSubResources bool) (*models.StreamGroup, error) {
	err := fmt.Errorf("the event streaming backend does not support stream groups")

	return nil, err
}

func (mgmt *emptyEventStreamMgmt) CreateMessageStreamGroupHandler(streamGroup *models.StreamGroup) (EventStreamer, error) {
	return emptyEventStreamer{}, nil
}

func (mgmt *emptyEventStreamMgmt) PublishMessage(request *v1.EventNotificationMessage, resource v1.CreateEventStreamingGroupRequest_EventResources) error {
	return nil
}

func (streamer emptyEventStreamer) GetResponseMessageChan() chan *v1.NotificationStreamGroupResponse {
	return make(chan *v1.NotificationStreamGroupResponse)
}

func (streamer emptyEventStreamer) StartStream() error {
	return nil
}

func (streamer emptyEventStreamer) CloseStream() error {
	return nil
}

func (streamer emptyEventStreamer) AckChunk(chunkID string) error {
	return nil
}
