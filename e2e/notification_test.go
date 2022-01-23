package e2e

import (
	"context"
	"fmt"
	"log"
	"net"
	"testing"
	"time"

	"github.com/ScienceObjectsDB/CORE-Server/eventstreaming"
	"github.com/ScienceObjectsDB/CORE-Server/models"
	v1 "github.com/ScienceObjectsDB/go-api/api/services/v1"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/encoding/protojson"
)

func TestNotificationStreamGroup(t *testing.T) {
	streamer1Count := 0
	streamer2Count := 0

	streamergroupID := uuid.New()
	subjectUUID := uuid.New()

	streamGroup := &models.StreamGroup{
		BaseModel: models.BaseModel{
			ID: streamergroupID,
		},
		Subject:        fmt.Sprintf("UPDATES.%v.>", subjectUUID),
		UseSubResource: true,
	}

	js, err := eventstreaming.NewNatsEventStreamMgmt(ServerEndpoints.dataset.ReadHandler, ServerEndpoints.dataset.CreateHandler)
	if err != nil {
		log.Fatalln(err.Error())
	}

	_, err = js.JetStreamContext.AddConsumer("UPDATES", &nats.ConsumerConfig{Durable: streamergroupID.String(), AckPolicy: nats.AckExplicitPolicy, FilterSubject: streamGroup.Subject})
	if err != nil {
		log.Fatalln(err.Error())
	}

	streamer1, err := ServerEndpoints.notification.EventStreamMgmt.CreateMessageStreamGroupHandler(streamGroup)
	if err != nil {
		log.Fatalln(err.Error())
	}

	streamer2, err := ServerEndpoints.notification.EventStreamMgmt.CreateMessageStreamGroupHandler(streamGroup)
	if err != nil {
		log.Fatalln(err.Error())
	}

	streamErrGrp := errgroup.Group{}

	streamErrGrp.Go(func() error {
		return streamer1.StartStream()
	})

	streamErrGrp.Go(func() error {
		return streamer2.StartStream()
	})

	streamErrGrp.Go(func() error {
		for msgs := range streamer1.GetResponseMessageChan() {
			streamer1Count = streamer1Count + len(msgs.Notification)
			streamer1.AckChunk(msgs.GetAckChunkId())
		}

		return nil
	})

	streamErrGrp.Go(func() error {
		for msgs := range streamer2.GetResponseMessageChan() {
			streamer2Count = streamer2Count + len(msgs.Notification)
			streamer2.AckChunk(msgs.GetAckChunkId())
		}

		return nil
	})

	streamErrGrp.Go(func() error {
		for i := 0; i < 4000; i++ {
			notification := &v1.EventNotificationMessage{
				Resource:    4,
				ResourceId:  fmt.Sprintf("test-%v", i),
				UpdatedType: v1.EventNotificationMessage_UPDATE_TYPE_CREATED,
			}

			data, err := protojson.Marshal(notification)
			if err != nil {
				log.Fatalln(err.Error())
			}

			_, err = js.JetStreamContext.Publish(fmt.Sprintf("UPDATES.%v.foo", subjectUUID), data)
			if err != nil {
				log.Fatalln(err.Error())
			}
		}

		err := streamer1.CloseStream()
		if err != nil {
			return err
		}

		err = streamer2.CloseStream()
		if err != nil {
			return err
		}

		return nil
	})

	streamErrGrp.Wait()

	assert.GreaterOrEqual(t, streamer1Count, 1)
	assert.GreaterOrEqual(t, streamer2Count, 1)
	assert.Equal(t, 4000, streamer1Count+streamer2Count)
}

var lis *bufconn.Listener

func TestResourceNotifications(t *testing.T) {
	bufSize := 1024 * 1024
	lis = bufconn.Listen(bufSize)
	s := grpc.NewServer()
	v1.RegisterUpdateNotificationServiceServer(s, ServerEndpoints.notification)
	go func() {
		if err := s.Serve(lis); err != nil {
			log.Fatalf("Server exited with error: %v", err)
		}
	}()

	ctx := context.Background()
	conn, err := grpc.DialContext(ctx, "bufnet", grpc.WithContextDialer(bufDialer), grpc.WithInsecure())
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}

	notificationClient := v1.NewUpdateNotificationServiceClient(conn)

	project, err := ServerEndpoints.project.CreateProject(context.Background(), &v1.CreateProjectRequest{
		Name:        "test",
		Description: "test",
	})
	if err != nil {
		t.Fatalf(err.Error())
	}

	streamGroup, err := notificationClient.CreateEventStreamingGroup(context.Background(), &v1.CreateEventStreamingGroupRequest{
		Resource:           v1.CreateEventStreamingGroupRequest_EVENT_RESOURCES_PROJECT_RESOURCE,
		ResourceId:         project.GetId(),
		IncludeSubresource: true,
		StreamType:         &v1.CreateEventStreamingGroupRequest_StreamAll{},
	})
	if err != nil {
		t.Fatalf(err.Error())
	}

	stream, err := notificationClient.NotificationStreamGroup(context.Background())
	if err != nil {
		t.Fatalf(err.Error())
	}

	err = stream.Send(&v1.NotificationStreamGroupRequest{
		StreamAction: &v1.NotificationStreamGroupRequest_Init{
			Init: &v1.NotificationStreamInit{
				StreamGroupId: streamGroup.StreamGroupId,
			},
		},
	})
	if err != nil {
		t.Fatalf(err.Error())
	}

	numberOfEventsChan := make(chan int, 10)

	errgrp := errgroup.Group{}
	errgrp.Go(func() error {
		for {
			events, err := stream.Recv()
			if err != nil {
				t.Fatalf(err.Error())
			}

			numberOfEventsChan <- len(events.Notification)

			err = stream.Send(&v1.NotificationStreamGroupRequest{
				StreamAction: &v1.NotificationStreamGroupRequest_Ack{
					Ack: &v1.NotficationStreamAck{
						AckChunkId: []string{events.GetAckChunkId()},
					},
				},
			})
			if err != nil {
				t.Fatalf(err.Error())
			}
		}
	})

	_, err = ServerEndpoints.dataset.CreateDataset(context.Background(), &v1.CreateDatasetRequest{
		Name:        "test",
		Description: "test",
		ProjectId:   project.GetId(),
	})
	if err != nil {
		t.Fatalf(err.Error())
	}

	sumNumberOfEvents := 0
	loops := 0

	for numberOfEvents := range numberOfEventsChan {
		loops++
		sumNumberOfEvents = sumNumberOfEvents + numberOfEvents
		if sumNumberOfEvents == 2 {
			break
		}

		if loops > 5 {
			t.Fatalf("could not find all expected events")
			break
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func bufDialer(context.Context, string) (net.Conn, error) {
	return lis.Dial()
}
