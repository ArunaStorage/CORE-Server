package server

import (
	"context"

	"github.com/google/uuid"
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

//CreateObjectGroup Creates a new object group endpoint service
func (endpoint *ObjectServerEndpoints) CreateObjectGroup(ctx context.Context, request *services.CreateObjectGroupRequest) (*services.CreateObjectGroupResponse, error) {
	parsedDatasetID, err := uuid.Parse(request.DatasetId)
	if err != nil {
		log.Debug(err.Error())
		return nil, status.Error(codes.InvalidArgument, "could not parse dataset id")
	}

	dataset, err := endpoint.ReadHandler.GetDataset(parsedDatasetID)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	metadata, _ := metadata.FromIncomingContext(ctx)

	err = endpoint.AuthzHandler.Authorize(
		dataset.ProjectID,
		protoModels.Right_WRITE,
		metadata)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	objectgroup, err := endpoint.CreateHandler.CreateObjectGroup(request)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	objectGroupResponse := &services.CreateObjectGroupResponse{
		ObjectGroupId:   objectgroup.ID.String(),
		ObjectGroupName: objectgroup.Name,
		ObjectLinks:     []*services.CreateObjectGroupResponse_ObjectLinks{},
	}
	if request.IncludeObjectLink {
		for _, object := range objectgroup.Objects {
			link, err := endpoint.ObjectHandler.CreateUploadLink(&object)
			if err != nil {
				log.Println(err.Error())
				return nil, err
			}
			objectGroupResponse.ObjectLinks = append(objectGroupResponse.ObjectLinks, &services.CreateObjectGroupResponse_ObjectLinks{
				Filename: object.Filename,
				Link:     link,
			})
		}
	}

	return objectGroupResponse, nil
}

func (endpoint *ObjectServerEndpoints) CreateObjectGroupBatch(ctx context.Context, requests *services.CreateObjectGroupBatchRequest) (*services.CreateObjectGroupBatchResponse, error) {
	if len(requests.GetRequests()) < 1 {
		return nil, status.Error(codes.InvalidArgument, "at least one request in request batch is required")
	}
	datasetID := requests.GetRequests()[0].DatasetId
	for _, request := range requests.GetRequests() {
		if datasetID != request.GetDatasetId() {
			return nil, status.Error(codes.InvalidArgument, "all requests have to have the same datasetid")
		}
	}

	parsedDatasetID, err := uuid.Parse(datasetID)
	if err != nil {
		log.Debug(err.Error())
		return nil, status.Error(codes.InvalidArgument, "could not parse dataset id")
	}

	dataset, err := endpoint.ReadHandler.GetDataset(parsedDatasetID)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	metadata, _ := metadata.FromIncomingContext(ctx)

	err = endpoint.AuthzHandler.Authorize(
		dataset.ProjectID,
		protoModels.Right_WRITE,
		metadata)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	objectgroups, err := endpoint.CreateHandler.CreateObjectGroupBatch(requests)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	var objectgroupResponseList []*services.CreateObjectGroupResponse

	for _, objectgroup := range objectgroups {
		objectgroupResponse := &services.CreateObjectGroupResponse{
			ObjectGroupId:   objectgroup.ID.String(),
			ObjectGroupName: objectgroup.Name,
			ObjectLinks:     make([]*services.CreateObjectGroupResponse_ObjectLinks, 0),
		}
		if requests.IncludeObjectLink {
			for _, object := range objectgroup.Objects {
				link, err := endpoint.ObjectHandler.CreateUploadLink(&object)
				if err != nil {
					log.Println(err.Error())
					return nil, err
				}

				objectgroupResponse.ObjectLinks = append(objectgroupResponse.ObjectLinks, &services.CreateObjectGroupResponse_ObjectLinks{
					Filename: object.Filename,
					Link:     link,
				})
			}
		}
		objectgroupResponseList = append(objectgroupResponseList, objectgroupResponse)
	}

	response := &services.CreateObjectGroupBatchResponse{
		Responses: objectgroupResponseList,
	}

	return response, nil
}

//GetObjectGroup Returns the object group with the given ID
func (endpoint *ObjectServerEndpoints) GetObjectGroup(ctx context.Context, request *services.GetObjectGroupRequest) (*services.GetObjectGroupResponse, error) {
	requestID, err := uuid.Parse(request.GetId())
	if err != nil {
		log.Debug(err.Error())
		return nil, status.Error(codes.InvalidArgument, "could not parse submitted id")
	}

	objectGroup, err := endpoint.ReadHandler.GetObjectGroup(requestID)
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
func (endpoint *ObjectServerEndpoints) FinishObjectUpload(ctx context.Context, request *services.FinishObjectUploadRequest) (*services.FinishObjectUploadResponse, error) {
	requestID, err := uuid.Parse(request.GetId())
	if err != nil {
		log.Debug(err.Error())
		return nil, status.Error(codes.InvalidArgument, "could not parse submitted id")
	}

	object, err := endpoint.ReadHandler.GetObject(requestID)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	metadata, _ := metadata.FromIncomingContext(ctx)

	err = endpoint.AuthzHandler.Authorize(
		object.ProjectID,
		protoModels.Right_WRITE,
		metadata)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	finished := &services.FinishObjectUploadResponse{}

	return finished, nil
}

func (endpoint *ObjectServerEndpoints) DeleteObjectGroup(ctx context.Context, request *services.DeleteObjectGroupRequest) (*services.DeleteObjectGroupResponse, error) {
	requestID, err := uuid.Parse(request.GetId())
	if err != nil {
		log.Debug(err.Error())
		return nil, status.Error(codes.InvalidArgument, "could not parse submitted id")
	}

	objectGroup, err := endpoint.ReadHandler.GetObjectGroup(requestID)
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

	objects, err := endpoint.ReadHandler.GetAllObjectGroupObjects(requestID)
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

	err = endpoint.DeleteHandler.DeleteObjectGroup(requestID)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	return &services.DeleteObjectGroupResponse{}, nil
}
