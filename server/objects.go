package server

import (
	"context"

	log "github.com/sirupsen/logrus"

	protoModels "github.com/ScienceObjectsDB/go-api/api/models/v1"
	services "github.com/ScienceObjectsDB/go-api/api/services/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type ObjectServerEndpoints struct {
	*Endpoints
}

func NewObjectEndpoints(endpoints *Endpoints) (*ObjectServerEndpoints, error) {
	objectEndpoints := &ObjectServerEndpoints{
		Endpoints: endpoints,
	}

	return objectEndpoints, nil
}

//CreateObjectGroup Creates a new object group
func (endpoint *ObjectServerEndpoints) CreateObjectGroup(ctx context.Context, request *services.CreateObjectGroupRequest) (*services.CreateObjectGroupResponse, error) {
	objectGroup, err := endpoint.ReadHandler.GetDataset(uint(request.GetDatasetId()))
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	metadata, _ := metadata.FromIncomingContext(ctx)

	err = endpoint.AuthzHandler.Authorize(
		objectGroup.ProjectID,
		protoModels.Right_READ,
		metadata)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	id, err := endpoint.CreateHandler.CreateObjectGroup(request)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	response := services.CreateObjectGroupResponse{
		ObjectGroupId: uint64(id),
	}

	return &response, nil
}

//GetObjectGroup Returns the object group with the given ID
func (endpoint *ObjectServerEndpoints) GetObjectGroup(ctx context.Context, request *services.GetObjectGroupRequest) (*services.GetObjectGroupResponse, error) {
	objectGroup, err := endpoint.ReadHandler.GetObjectGroup(uint(request.GetId()))
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	metadata, _ := metadata.FromIncomingContext(ctx)

	err = endpoint.AuthzHandler.Authorize(
		objectGroup.ProjectID,
		protoModels.Right_READ,
		metadata)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	protoObjectGroup := objectGroup.ToProtoModel()
	response := services.GetObjectGroupResponse{
		ObjectGroup: protoObjectGroup,
	}

	return &response, nil
}

//FinishObjectUpload Finishes the upload process for an object
func (endpoint *ObjectServerEndpoints) FinishObjectUpload(_ context.Context, _ *services.FinishObjectUploadRequest) (*services.FinishObjectUploadResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (endpoint *ObjectServerEndpoints) DeleteObjectGroup(ctx context.Context, request *services.DeleteObjectGroupRequest) (*services.DeleteObjectGroupResponse, error) {
	objectGroup, err := endpoint.ReadHandler.GetObjectGroup(uint(request.GetId()))
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	metadata, _ := metadata.FromIncomingContext(ctx)

	err = endpoint.AuthzHandler.Authorize(
		objectGroup.ProjectID,
		protoModels.Right_WRITE,
		metadata)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	objects, err := endpoint.ReadHandler.GetAllObjectGroupObjects(uint(request.GetId()))
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	if len(objects) != 0 {
		err = endpoint.ObjectHandler.DeleteObjects(objects)
		if err != nil {
			log.Println(err.Error())
			return nil, err
		}
	}

	err = endpoint.DeleteHandler.DeleteObjectGroup(uint(request.GetId()))
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	return &services.DeleteObjectGroupResponse{}, nil
}
