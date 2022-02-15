package eventstreaming

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ScienceObjectsDB/CORE-Server/config"
	"github.com/ScienceObjectsDB/CORE-Server/database"
	"github.com/ScienceObjectsDB/CORE-Server/models"
	"github.com/nats-io/nats.go"

	v1notificationservices "github.com/ScienceObjectsDB/go-api/sciobjsdb/api/notification/services/v1"
	v1storagemodels "github.com/ScienceObjectsDB/go-api/sciobjsdb/api/storage/models/v1"
)

const OBJECTGROUPSUBJECTNAME = "objectgroup"
const DATASETVERSIONSUBJECTNAME = "datasetversion"
const DEFAULTSUBJECTSUFFIX = "_"

type NatsEventStreamMgmt struct {
	Connection       *nats.Conn
	JetStreamContext nats.JetStreamContext
	JetStreamManager nats.JetStreamManager
	DatabaseRead     *database.Read
	DatabaseCreate   *database.Create
	SubjectPrefix    string
}

func NewNatsEventStreamMgmt(databaseReader *database.Read, databaseCreate *database.Create) (*NatsEventStreamMgmt, error) {
	urls := viper.GetStringSlice(config.EVENTNOTIFICATION_NATS_HOST)
	streamSubjectPrefix := viper.GetString(config.EVENTNOTIFICATION_NATS_SUBJECTPREFIX)

	var serverstring string
	if len(urls) == 1 {
		serverstring = urls[0]
	} else if len(urls) > 1 {
		serverstring = strings.Join(urls, ", ")
	} else {
		serverstring = nats.DefaultURL
	}

	var options []nats.Option

	if viper.IsSet(config.EVENTNOTIFICATION_NATS_NKeySeedFileName) {
		nkeySeedFile := viper.GetString(config.EVENTNOTIFICATION_NATS_NKeySeedFileName)
		nkeyopts, err := nats.NkeyOptionFromSeed(nkeySeedFile)
		if err != nil {
			log.Errorln(err.Error())
			return nil, err
		}
		options = append(options, nkeyopts)
	}
	options = append(options, nats.Timeout(5*time.Second))

	nc, err := nats.Connect(serverstring, options...)
	if err != nil {
		log.Errorln(err.Error())
		return nil, err
	}

	jetstream, err := nc.JetStream()
	if err != nil {
		log.Errorln(err.Error())
		return nil, err
	}

	streaming := &NatsEventStreamMgmt{
		Connection:       nc,
		JetStreamContext: jetstream,
		JetStreamManager: jetstream,
		SubjectPrefix:    streamSubjectPrefix,
		DatabaseRead:     databaseReader,
		DatabaseCreate:   databaseCreate,
	}

	return streaming, nil
}

func (eventStreamManager *NatsEventStreamMgmt) CreateMessageStreamGroupHandler(streamGroup *models.StreamGroup) (EventStreamer, error) {
	sub, err := eventStreamManager.JetStreamContext.PullSubscribe(streamGroup.Subject, streamGroup.ID.String(), nats.Bind(eventStreamManager.SubjectPrefix, streamGroup.ID.String()))
	if err != nil {
		log.Errorln(err.Error())
		return nil, err
	}

	responseMsgChan := make(chan *v1notificationservices.NotificationStreamGroupResponse, 3)

	streamer := &NatsEventStreamer{
		ResponseMsgChan: responseMsgChan,
		Subscription:    sub,
		MsgMap:          make(map[string][]*nats.Msg),
		MsgMapMutex:     &sync.Mutex{},
		MaxPendingAck:   make(chan bool, 3),
		Close:           make(chan bool, 1),
	}

	return streamer, nil
}

func (eventStreamManager *NatsEventStreamMgmt) CreateStreamGroup(projectID uuid.UUID, resourceID uuid.UUID, resourceType *v1notificationservices.CreateEventStreamingGroupRequest_EventResources, includeSubResources bool) (*models.StreamGroup, error) {
	targetSubject, err := eventStreamManager.getSubscriptionSubject(resourceID, *resourceType, includeSubResources)
	if err != nil {
		log.Errorln(err.Error())
		return nil, err
	}

	group, err := eventStreamManager.DatabaseCreate.CreateStreamGroup(projectID, resourceType.Enum().String(), resourceID, targetSubject, includeSubResources)
	if err != nil {
		log.Errorln(err.Error())
		return nil, err
	}

	cfg := &nats.ConsumerConfig{
		Durable:       group.ID.String(),
		FilterSubject: targetSubject,
		DeliverPolicy: nats.DeliverAllPolicy,
		AckPolicy:     nats.AckExplicitPolicy,
		AckWait:       time.Second * 15,
	}

	_, err = eventStreamManager.JetStreamManager.AddConsumer(viper.GetString(config.EVENTNOTIFICATION_NATS_STREAM_NAME), cfg)
	if err != nil {
		log.Errorln(err.Error())
		return nil, err
	}

	return group, err
}

func (eventStreamManager *NatsEventStreamMgmt) PublishMessage(request *v1notificationservices.EventNotificationMessage, resource v1notificationservices.CreateEventStreamingGroupRequest_EventResources) error {
	data, err := protojson.Marshal(request)
	if err != nil {
		log.Errorln(err.Error())
		return err
	}

	publishSubject, err := eventStreamManager.getPublishSubject(request)
	if err != nil {
		log.Errorln(err.Error())
		return err
	}

	_, err = eventStreamManager.JetStreamContext.Publish(publishSubject, data)
	if err != nil {
		log.Errorln(err.Error())
		return err
	}

	return nil
}

func (eventStreamManager *NatsEventStreamMgmt) EnableTestMode() error {
	targetSubject := fmt.Sprintf("%v.>", viper.GetString(config.EVENTNOTIFICATION_NATS_SUBJECTPREFIX))

	_, err := eventStreamManager.JetStreamContext.AddStream(&nats.StreamConfig{Name: viper.GetString(config.EVENTNOTIFICATION_NATS_STREAM_NAME), Subjects: []string{targetSubject}, Storage: nats.MemoryStorage})
	if err != nil {
		log.Errorln(err.Error())
		return err
	}

	return nil
}

func (eventStreamManager *NatsEventStreamMgmt) getSubscriptionSubject(resourceID uuid.UUID, resourceType v1notificationservices.CreateEventStreamingGroupRequest_EventResources, useSubResource bool) (string, error) {
	subject := ""

	finalSymbol := DEFAULTSUBJECTSUFFIX
	if useSubResource {
		finalSymbol = ">"
	}

	switch resourceType {
	case v1notificationservices.CreateEventStreamingGroupRequest_EVENT_RESOURCES_PROJECT_RESOURCE:
		{
			subject = fmt.Sprintf("%v.%v.%v", eventStreamManager.SubjectPrefix, resourceID.String(), finalSymbol)
		}

	case v1notificationservices.CreateEventStreamingGroupRequest_EVENT_RESOURCES_DATASET_RESOURCE:
		{
			dataset, err := eventStreamManager.DatabaseRead.GetDataset(resourceID)
			if err != nil {
				log.Errorln(err.Error())
				return "", err
			}

			subject = fmt.Sprintf("%v.%v.%v.%v", eventStreamManager.SubjectPrefix, dataset.ProjectID.String(), dataset.ID.String(), finalSymbol)

		}

	case v1notificationservices.CreateEventStreamingGroupRequest_EVENT_RESOURCES_DATASET_VERSION_RESOURCE:
		{
			datasetVersion, err := eventStreamManager.DatabaseRead.GetDatasetVersion(resourceID)
			if err != nil {
				log.Errorln(err.Error())
				return "", err
			}

			subject = fmt.Sprintf("%v.%v.%v.%v.%v.%v", eventStreamManager.SubjectPrefix, datasetVersion.ProjectID.String(), datasetVersion.DatasetID.String(), DATASETVERSIONSUBJECTNAME, datasetVersion.ID.String(), finalSymbol)
		}

	case v1notificationservices.CreateEventStreamingGroupRequest_EVENT_RESOURCES_OBJECT_GROUP_RESOURCE:
		{
			objectGroup, err := eventStreamManager.DatabaseRead.GetObjectGroup(resourceID)
			if err != nil {
				log.Errorln(err.Error())
				return "", err
			}

			subject = fmt.Sprintf("%v.%v.%v.%v.%v.%v", eventStreamManager.SubjectPrefix, objectGroup.ProjectID.String(), objectGroup.DatasetID.String(), OBJECTGROUPSUBJECTNAME, objectGroup.ID.String(), finalSymbol)
		}

	default:
		{
			return "", fmt.Errorf("queried resource not implemented")
		}
	}

	return subject, nil
}

func (eventStreamManager *NatsEventStreamMgmt) getPublishSubject(request *v1notificationservices.EventNotificationMessage) (string, error) {
	idAsUUID, err := uuid.Parse(request.ResourceId)
	if err != nil {
		log.Errorln(err.Error())
		return "", err
	}

	switch request.Resource {
	case v1storagemodels.Resource_RESOURCE_PROJECT:
		{
			subject := fmt.Sprintf("%v.%v._", eventStreamManager.SubjectPrefix, request.ResourceId)
			return subject, nil
		}
	case v1storagemodels.Resource_RESOURCE_DATASET:
		{
			dataset, err := eventStreamManager.DatabaseRead.GetDataset(idAsUUID)
			if err != nil {
				log.Errorln(err.Error())
				return "", err
			}

			subject := fmt.Sprintf("%v.%v.%v._", eventStreamManager.SubjectPrefix, dataset.ProjectID.String(), dataset.ID.String())

			return subject, nil
		}

	case v1storagemodels.Resource_RESOURCE_OBJECT_GROUP:
		{
			objectgroup, err := eventStreamManager.DatabaseRead.GetObjectGroup(idAsUUID)
			if err != nil {
				log.Errorln(err.Error())
				return "", err
			}

			subject := fmt.Sprintf("%v.%v.%v.%v.%v._", eventStreamManager.SubjectPrefix, objectgroup.ProjectID.String(), objectgroup.DatasetID.String(), OBJECTGROUPSUBJECTNAME, objectgroup.ID.String())

			return subject, nil
		}
	case v1storagemodels.Resource_RESOURCE_DATASET_VERSION:
		{
			datasetVersion, err := eventStreamManager.DatabaseRead.GetDatasetVersion(idAsUUID)
			if err != nil {
				log.Errorln(err.Error())
				return "", err
			}

			subject := fmt.Sprintf("%v.%v.%v.%v.%v._", eventStreamManager.SubjectPrefix, datasetVersion.ProjectID.String(), datasetVersion.DatasetID.String(), DATASETVERSIONSUBJECTNAME, datasetVersion.ID.String())

			return subject, nil
		}

	default:
		{
			return "", fmt.Errorf("provided resource not implemented")
		}
	}

}

type NatsEventStreamer struct {
	Subscription    *nats.Subscription
	ResponseMsgChan chan *v1notificationservices.NotificationStreamGroupResponse
	MsgMap          map[string][]*nats.Msg
	MsgMapMutex     *sync.Mutex
	MaxPendingAck   chan bool
	Close           chan bool
	ID              string
}

func (streamer *NatsEventStreamer) GetResponseMessageChan() chan *v1notificationservices.NotificationStreamGroupResponse {
	return streamer.ResponseMsgChan
}

func (streamer *NatsEventStreamer) StartStream() error {
	for {
		var responseChunk []*v1notificationservices.NotificationStreamResponse

		select {
		case <-streamer.Close:
			close(streamer.ResponseMsgChan)
			return nil
		default:
		}

		streamer.MaxPendingAck <- true

		chunk, err := streamer.Subscription.Fetch(500, nats.MaxWait(1*time.Second))
		if err != nil && err != nats.ErrTimeout {
			log.Errorln(err.Error())
			return err
		}

		for _, msg := range chunk {
			notificationMsg := &v1notificationservices.EventNotificationMessage{}

			err := protojson.Unmarshal(msg.Data, notificationMsg)
			if err != nil {
				log.Errorln(err.Error())
				return err
			}

			metadata, err := msg.Metadata()
			if err != nil {
				log.Errorln(err.Error())
				return err
			}

			responseMsg := &v1notificationservices.NotificationStreamResponse{
				Message:   notificationMsg,
				Sequence:  metadata.Sequence.Stream,
				Timestamp: timestamppb.Now(),
			}

			responseChunk = append(responseChunk, responseMsg)
		}

		ackUUID := uuid.New()

		response := &v1notificationservices.NotificationStreamGroupResponse{
			Notification: responseChunk,
			AckChunkId:   ackUUID.String(),
		}

		streamer.ResponseMsgChan <- response

		streamer.MsgMapMutex.Lock()
		streamer.MsgMap[ackUUID.String()] = chunk
		streamer.MsgMapMutex.Unlock()
	}
}

func (streamer *NatsEventStreamer) CloseStream() error {
	streamer.Close <- true

	return nil
}

func (streamer *NatsEventStreamer) AckChunk(chunkID string) error {
	streamer.MsgMapMutex.Lock()
	response := streamer.MsgMap[chunkID]
	delete(streamer.MsgMap, chunkID)
	streamer.MsgMapMutex.Unlock()

	for _, msg := range response {
		err := msg.Ack()
		if err != nil {
			log.Errorln(err.Error())
			return err
		}
	}

	<-streamer.MaxPendingAck

	return nil
}
