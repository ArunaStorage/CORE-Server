package eventstreaming

import (
	"fmt"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ScienceObjectsDB/CORE-Server/database"
	v1 "github.com/ScienceObjectsDB/go-api/api/services/v1"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
)

type NatsEventStreamMgmt struct {
	Connection       *nats.Conn
	JetStreamContext nats.JetStream
	JetStreamManager nats.JetStreamManager
	ReadHandler      *database.Read
	SubjectPrefix    string
}

type NatsEventStreamer struct {
	MsgChan          chan *nats.Msg
	ResponseChan     chan *v1.NotificationStreamResponse
	ReadHandler      *database.Read
	JetStreamContext nats.JetStream
	Subscription     *nats.Subscription
}

func NewNatsEventStreamMgmt(databaseReader *database.Read) (*NatsEventStreamMgmt, error) {
	urls := viper.GetStringSlice("EventNotifications.NATS.URL")
	streamSubjectPrefix := viper.GetString("EventNotifications.NATS.SubjectPrefix")

	var serverstring string
	if len(urls) == 1 {
		serverstring = urls[0]
	} else if len(urls) > 1 {
		serverstring = strings.Join(urls, ", ")
	} else {
		serverstring = nats.DefaultURL
	}

	var options []nats.Option

	if viper.IsSet("EventNotifications.NATS.NKeySeedFileName") {
		nkeySeedFile := viper.GetString("EventNotifications.NATS.NKeySeedFileName")
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
		ReadHandler:      databaseReader,
		SubjectPrefix:    streamSubjectPrefix,
	}

	return streaming, nil
}

func (streaming *NatsEventStreamMgmt) PublishMessage(msg *v1.EventNotificationMessage, resource v1.NotificationStreamRequest_EventResources) error {
	data, err := proto.Marshal(msg)
	if err != nil {
		log.Errorln(err.Error())
		return err
	}

	subject, err := streaming.createStreamSubject(msg.GetResourceId(), resource, false)
	if err != nil {
		log.Errorln(err.Error())
		return err
	}

	_, err = streaming.JetStreamContext.Publish(subject, data)
	if err != nil {
		log.Errorln(err.Error())
		return err
	}

	return nil
}

func (streaming *NatsEventStreamMgmt) getStreamOptions(request *v1.NotificationStreamRequest) (nats.SubOpt, error) {
	var deliverOption nats.SubOpt

	switch value := request.StreamType.(type) {
	case *v1.NotificationStreamRequest_StreamAll:
		deliverOption = nats.DeliverAll()
	case *v1.NotificationStreamRequest_StreamFromDate:
		deliverOption = nats.StartTime(value.StreamFromDate.Timestamp.AsTime())
	case *v1.NotificationStreamRequest_StreamFromSequence:
		deliverOption = nats.StartSequence(value.StreamFromSequence.GetSequence())
	}

	return deliverOption, nil
}

func (streaming *NatsEventStreamMgmt) createStreamSubject(resourceID string, resource v1.NotificationStreamRequest_EventResources, includeSubResources bool) (string, error) {
	resourceUUID, err := uuid.Parse(resourceID)
	if err != nil {
		log.Errorln(err)
		return "", status.Error(codes.InvalidArgument, "could not parse provided resource id as uuid")
	}

	var idList []string

	switch resource {
	case v1.NotificationStreamRequest_EVENT_RESOURCES_PROJECT_RESOURCE:
		project, err := streaming.ReadHandler.GetProject(resourceUUID)
		if err != nil {
			log.Errorln(err)
			return "", status.Error(codes.Internal, "could not find project")
		}
		idList = append(idList, project.ID.String())
	case v1.NotificationStreamRequest_EVENT_RESOURCES_DATASET_RESOURCE:
		dataset, err := streaming.ReadHandler.GetDataset(resourceUUID)
		if err != nil {
			log.Errorln(err)
			return "", status.Error(codes.Internal, "could not find dataset")
		}
		idList = append(idList, dataset.ProjectID.String(), dataset.ID.String())
	case v1.NotificationStreamRequest_EVENT_RESOURCES_OBJECT_GROUP_RESOURCE:
		objectGroup, err := streaming.ReadHandler.GetObjectGroup(resourceUUID)
		if err != nil {
			log.Errorln(err)
			return "", status.Error(codes.Internal, "could not find dataset")
		}
		idList = append(idList, objectGroup.ProjectID.String(), objectGroup.DatasetID.String(), objectGroup.ID.String())
	default:
		return "", status.Error(codes.Unimplemented, fmt.Sprintf("resource type %v not implemented", resource.String()))
	}

	if includeSubResources {
		idList = append(idList, "*")
	}

	idConcat := strings.Join(idList, ".")
	subjectFullName := fmt.Sprintf("%v.%v", streaming.SubjectPrefix, idConcat)

	log.Debugln(subjectFullName)

	return subjectFullName, nil
}

func (streaming *NatsEventStreamMgmt) CreateMessageStreamHandler(request *v1.NotificationStreamRequest) (EventStreamer, error) {
	streamMessageChan := make(chan *nats.Msg, 1000)
	streamResponseChan := make(chan *v1.NotificationStreamResponse, 1000)

	subject, err := streaming.createStreamSubject(request.GetResourceId(), request.GetResource(), request.GetIncludeSubresource())
	if err != nil {
		return nil, err
	}

	streamOpts, err := streaming.getStreamOptions(request)
	if err != nil {
		return nil, err
	}

	sub, err := streaming.JetStreamContext.ChanSubscribe(subject, streamMessageChan, streamOpts)
	if err != nil {
		log.Fatalln(err.Error())
		return nil, err
	}

	eventStreamer := &NatsEventStreamer{
		ResponseChan:     streamResponseChan,
		MsgChan:          streamMessageChan,
		ReadHandler:      streaming.ReadHandler,
		JetStreamContext: streaming.JetStreamContext,
		Subscription:     sub,
	}

	return eventStreamer, nil
}

func (streaming *NatsEventStreamer) GetResponseMessageChan() chan *v1.NotificationStreamResponse {
	return streaming.ResponseChan
}

func (streaming *NatsEventStreamer) StartMessageTransformation() error {
	for msg := range streaming.MsgChan {
		meta, err := msg.Metadata()
		if err != nil {
			log.Errorln(err.Error())
			return status.Error(codes.Internal, "error while parsing event message metadata")
		}

		notificationMsg := &v1.EventNotificationMessage{}
		err = proto.Unmarshal(msg.Data, notificationMsg)
		if err != nil {
			log.Errorln(err.Error())
			return status.Error(codes.Internal, "error while parsing event message")
		}

		response := v1.NotificationStreamResponse{
			Message:   notificationMsg,
			Sequence:  meta.Sequence.Stream,
			Timestamp: timestamppb.New(meta.Timestamp),
		}

		streaming.ResponseChan <- &response
	}

	return nil
}
