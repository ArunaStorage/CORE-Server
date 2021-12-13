package server

import (
	"context"

	"github.com/ScienceObjectsDB/CORE-Server/eventstreaming"
	services "github.com/ScienceObjectsDB/go-api/api/services/v1"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type NotificationEndpoints struct {
	*Endpoints
}

// NewLoadEndpoints New load service
func NewNotificationEndpoints(endpoints *Endpoints, eventStreamMgmt eventstreaming.EventStreamMgmt) (*NotificationEndpoints, error) {
	notificationEndpoints := &NotificationEndpoints{
		Endpoints: endpoints,
	}

	return notificationEndpoints, nil
}

func (endpoint NotificationEndpoints) NotificationStream(request *services.NotificationStreamRequest, stream services.UpdateNotificationService_NotificationStreamServer) error {
	streamer, err := endpoint.EventStreamMgmt.CreateMessageStreamHandler(request)
	if err != nil {
		log.Errorln(err.Error())
		return status.Error(codes.Internal, "error when trying to connect to the event stream")
	}

	responseMsgChan := streamer.GetResponseMessageChan()

	errgrp, _ := errgroup.WithContext(context.Background())

	errgrp.Go(streamer.StartMessageTransformation)
	errgrp.Go(func() error {
		for eventNotificationResponse := range responseMsgChan {
			err := stream.Send(eventNotificationResponse)
			if err != nil {
				log.Errorln(err.Error())
				return err
			}
		}
		return nil
	})

	err = errgrp.Wait()
	if err != nil {
		return status.Error(codes.Internal, "error while sending responses to stream")
	}

	return nil
}
