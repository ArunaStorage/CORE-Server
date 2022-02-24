package eventstreaming

import (
	"fmt"

	"github.com/ScienceObjectsDB/CORE-Server/models"
	v1notificationservices "github.com/ScienceObjectsDB/go-api/sciobjsdb/api/notification/services/v1"
	"github.com/google/uuid"
)

type emptyEventStreamMgmt struct {
}

type emptyEventStreamer struct {
}

func (mgmt *emptyEventStreamMgmt) EnableTestMode() error {
	return nil
}

func (mgmt *emptyEventStreamMgmt) CreateStreamGroup(projectID uuid.UUID, resourceID uuid.UUID, resourceType *v1notificationservices.CreateEventStreamingGroupRequest_EventResources, includeSubResources bool) (*models.StreamGroup, error) {
	err := fmt.Errorf("the event streaming backend does not support stream groups")

	return nil, err
}

func (mgmt *emptyEventStreamMgmt) CreateMessageStreamGroupHandler(streamGroup *models.StreamGroup) (EventStreamer, error) {
	return emptyEventStreamer{}, nil
}

func (mgmt *emptyEventStreamMgmt) PublishMessage(request *v1notificationservices.EventNotificationMessage) error {
	return nil
}

func (streamer emptyEventStreamer) GetResponseMessageChan() chan *v1notificationservices.NotificationStreamGroupResponse {
	return make(chan *v1notificationservices.NotificationStreamGroupResponse)
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
