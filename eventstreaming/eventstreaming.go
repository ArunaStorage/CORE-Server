package eventstreaming

import (
	"fmt"

	"github.com/ScienceObjectsDB/CORE-Server/config"
	"github.com/ScienceObjectsDB/CORE-Server/database"
	"github.com/ScienceObjectsDB/CORE-Server/models"
	v1notificationservices "github.com/ScienceObjectsDB/go-api/sciobjsdb/api/notification/services/v1"
	"github.com/google/uuid"
	"github.com/spf13/viper"
)

func New(dbRead *database.Read, dbCreate *database.Create) (EventStreamMgmt, error) {
	eventStreamBackendConfString := viper.GetString(config.EVENTNOTIFICATION_BACKEND)

	var streamMgmt EventStreamMgmt
	var err error

	switch eventStreamBackendConfString {
	case "Empty":
		streamMgmt = &emptyEventStreamMgmt{}
	case "NATS":
		streamMgmt, err = NewNatsEventStreamMgmt(dbRead, dbCreate)
	default:
		err = fmt.Errorf("no valid eventstreaming config found in EventNotifications.Backend, please specify either NATS or Empty")
	}

	return streamMgmt, err
}

type EventStreamMgmt interface {
	CreateMessageStreamGroupHandler(streamGroup *models.StreamGroup) (EventStreamer, error)
	CreateStreamGroup(projectID uuid.UUID, resourceID uuid.UUID, resourceType *v1notificationservices.CreateEventStreamingGroupRequest_EventResources, includeSubResources bool) (*models.StreamGroup, error)
	PublishMessage(request *v1notificationservices.EventNotificationMessage) error
	EnableTestMode() error
}

type EventStreamer interface {
	GetResponseMessageChan() chan *v1notificationservices.NotificationStreamGroupResponse
	StartStream() error
	CloseStream() error
	AckChunk(chunkID string) error
}
