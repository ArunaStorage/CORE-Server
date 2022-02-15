package server

import (
	"context"
	"io"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	v1notficationservices "github.com/ScienceObjectsDB/go-api/sciobjsdb/api/notification/services/v1"
	v1storagemodels "github.com/ScienceObjectsDB/go-api/sciobjsdb/api/storage/models/v1"
)

type NotificationEndpoints struct {
	*Endpoints
}

// NewLoadEndpoints New load service
func NewNotificationEndpoints(endpoints *Endpoints) (*NotificationEndpoints, error) {
	notificationEndpoints := &NotificationEndpoints{
		Endpoints: endpoints,
	}

	return notificationEndpoints, nil
}

func (notificationEndpoints *NotificationEndpoints) CreateEventStreamingGroup(ctx context.Context, request *v1notficationservices.CreateEventStreamingGroupRequest) (*v1notficationservices.CreateEventStreamingGroupResponse, error) {
	metadata, _ := metadata.FromIncomingContext(ctx)

	var projectUUID uuid.UUID

	resourceUUID, err := uuid.Parse(request.ResourceId)
	if err != nil {
		log.Errorln(err.Error())
		return nil, status.Error(codes.InvalidArgument, "could not parse resource id into uuid")
	}

	switch request.Resource {
	case v1notficationservices.CreateEventStreamingGroupRequest_EVENT_RESOURCES_PROJECT_RESOURCE:
		{
			projectUUID = resourceUUID
		}
	case v1notficationservices.CreateEventStreamingGroupRequest_EVENT_RESOURCES_DATASET_RESOURCE:
		{
			dataset, err := notificationEndpoints.ReadHandler.GetDataset(resourceUUID)
			if err != nil {
				log.Errorln(err.Error())
				return nil, err
			}

			projectUUID = dataset.ProjectID
		}
	default:
		{
			return nil, status.Error(codes.InvalidArgument, "resource type not supported")
		}
	}

	err = notificationEndpoints.AuthzHandler.Authorize(
		projectUUID,
		v1storagemodels.Right_RIGHT_WRITE,
		metadata)
	if err != nil {
		log.Errorln(err.Error())
		return nil, err
	}

	streamGroup, err := notificationEndpoints.EventStreamMgmt.CreateStreamGroup(projectUUID, resourceUUID, &request.Resource, request.IncludeSubresource)
	if err != nil {
		log.Errorln(err.Error())
		return nil, err
	}

	response := &v1notficationservices.CreateEventStreamingGroupResponse{
		StreamGroupId: streamGroup.ID.String(),
	}

	return response, nil
}

func (notificationEndpoints *NotificationEndpoints) NotificationStreamGroup(stream v1notficationservices.UpdateNotificationService_NotificationStreamGroupServer) error {
	metadata, _ := metadata.FromIncomingContext(stream.Context())

	request, err := stream.Recv()
	if err != nil {
		log.Errorln(err.Error())
		return err
	}

	if request.GetInit() == nil {
		return status.Error(codes.Internal, "first message needs to be init")
	}

	init := request.GetInit()

	streamGroupUUID, err := uuid.Parse(init.StreamGroupId)
	if err != nil {
		log.Errorln(err.Error())
		return err
	}

	streamGroup, err := notificationEndpoints.ReadHandler.GetStreamGroup(streamGroupUUID)
	if err != nil {
		log.Errorln(err.Error())
		return err
	}

	err = notificationEndpoints.AuthzHandler.Authorize(
		streamGroup.ProjectID,
		v1storagemodels.Right_RIGHT_READ,
		metadata)
	if err != nil {
		log.Errorln(err.Error())
		return err
	}

	internalStreamer, err := notificationEndpoints.EventStreamMgmt.CreateMessageStreamGroupHandler(streamGroup)
	if err != nil {
		log.Errorln(err.Error())
		return err
	}

	errgrp := &errgroup.Group{}

	errgrp.Go(func() error {
		err = internalStreamer.StartStream()
		if err != nil {
			log.Errorln(err.Error())
			return err
		}

		return nil
	})

	errgrp.Go(func() error {
		for notification := range internalStreamer.GetResponseMessageChan() {
			err = stream.Send(notification)
			if err == io.EOF {
				return nil
			}

			if err != nil {
				log.Errorln(err.Error())
				return err
			}
		}

		return status.Error(codes.Internal, "internal channel closed unexpectedly")
	})

	errgrp.Go(func() error {
		for {
			request, err := stream.Recv()
			if err == io.EOF {
				return nil
			}

			if err != nil {
				log.Errorln(err.Error())
				return err
			}

			ackRequest := request.GetAck()
			if ackRequest == nil {
				return status.Error(codes.InvalidArgument, "ack required, but ack field in request was nil")
			}

			for _, ackChunkID := range ackRequest.GetAckChunkId() {
				err = internalStreamer.AckChunk(ackChunkID)
				if err != nil {
					log.Errorln(err.Error())
					return err
				}
			}

			if request.GetClose() {
				err := internalStreamer.CloseStream()
				if err != nil {
					log.Errorln(err.Error())
					return err
				}
			}
		}
	})

	err = errgrp.Wait()
	if err != nil {
		log.Errorln(err.Error())
		return err
	}

	return nil
}
