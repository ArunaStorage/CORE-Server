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

type DatasetEndpoints struct {
	*Endpoints
}

func NewDatasetEndpoints(endpoints *Endpoints) (*DatasetEndpoints, error) {
	datasetEndpoint := &DatasetEndpoints{
		Endpoints: endpoints,
	}

	return datasetEndpoint, nil
}

// CreateNewDataset Creates a new dataset and associates it with a dataset
func (endpoint *DatasetEndpoints) CreateDataset(ctx context.Context, request *services.CreateDatasetRequest) (*services.CreateDatasetResponse, error) {
	metadata, _ := metadata.FromIncomingContext(ctx)

	err := endpoint.AuthzHandler.Authorize(
		uint(request.GetProjectId()),
		protoModels.Right_WRITE,
		metadata)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	id, err := endpoint.CreateHandler.CreateDataset(request)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	response := services.CreateDatasetResponse{
		Id: uint64(id),
	}

	return &response, nil
}

// Dataset Returns a specific dataset
func (endpoint *DatasetEndpoints) GetDataset(ctx context.Context, request *services.GetDatasetRequest) (*services.GetDatasetResponse, error) {
	dataset, err := endpoint.ReadHandler.GetDataset(uint(request.GetId()))
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	metadata, _ := metadata.FromIncomingContext(ctx)

	err = endpoint.AuthzHandler.Authorize(
		uint(dataset.ProjectID),
		protoModels.Right_READ,
		metadata)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	protoDataset := dataset.ToProtoModel()
	response := services.GetDatasetResponse{
		Dataset: &protoDataset,
	}

	return &response, nil
}

// Lists Versions of a dataset
func (endpoint *DatasetEndpoints) GetDatasetVersions(ctx context.Context, request *services.GetDatasetVersionsRequest) (*services.GetDatasetVersionsResponse, error) {
	dataset, err := endpoint.ReadHandler.GetDataset(uint(request.GetId()))
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	metadata, _ := metadata.FromIncomingContext(ctx)

	err = endpoint.AuthzHandler.Authorize(
		uint(dataset.ProjectID),
		protoModels.Right_READ,
		metadata)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	versions, err := endpoint.ReadHandler.GetDatasetVersions(uint(request.GetId()))
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	var protoVersions []*protoModels.DatasetVersion
	for _, version := range versions {
		protoVersion := version.ToProtoModel()
		protoVersions = append(protoVersions, protoVersion)
	}

	response := &services.GetDatasetVersionsResponse{
		DatasetVersions: protoVersions,
	}

	return response, nil
}

func (endpoint *DatasetEndpoints) GetDatasetObjectGroups(ctx context.Context, request *services.GetDatasetObjectGroupsRequest) (*services.GetDatasetObjectGroupsResponse, error) {
	dataset, err := endpoint.ReadHandler.GetDataset(uint(request.GetId()))
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	metadata, _ := metadata.FromIncomingContext(ctx)

	err = endpoint.AuthzHandler.Authorize(
		uint(dataset.ProjectID),
		protoModels.Right_READ,
		metadata)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	objectGroups, err := endpoint.ReadHandler.GetDatasetObjectGroups(uint(request.GetId()))
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	var protoObjectGroups []*protoModels.ObjectGroup
	for _, objectGroup := range objectGroups {
		protoObjectGroup := objectGroup.ToProtoModel()
		protoObjectGroups = append(protoObjectGroups, protoObjectGroup)
	}

	response := services.GetDatasetObjectGroupsResponse{
		ObjectGroups: protoObjectGroups,
	}

	return &response, nil
}

func (endpoint *DatasetEndpoints) GetObjectGroupsInDateRange(ctx context.Context, request *services.GetObjectGroupsInDateRangeRequest) (*services.GetObjectGroupsInDateRangeResponse, error) {
	dataset, err := endpoint.ReadHandler.GetDataset(uint(request.GetId()))
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	metadata, _ := metadata.FromIncomingContext(ctx)

	err = endpoint.AuthzHandler.Authorize(
		uint(dataset.ProjectID),
		protoModels.Right_READ,
		metadata)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	objectGroups, err := endpoint.ReadHandler.GetObjectGroupsInDateRange(dataset.ID, request.Start.AsTime(), request.End.AsTime())
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	var protoObjectGroups []*protoModels.ObjectGroup
	for _, object := range objectGroups {
		protoObjectGroups = append(protoObjectGroups, object.ToProtoModel())
	}

	response := &services.GetObjectGroupsInDateRangeResponse{
		ObjectGroups: protoObjectGroups,
	}

	return response, nil
}

func (endpoint *DatasetEndpoints) GetObjectGroupsStream(ctx context.Context, request *services.GetObjectGroupsStreamLinkRequest) (*services.GetObjectGroupsStreamLinkResponse, error) {
	var projectID uint
	switch value := request.Query.(type) {
	case *services.GetObjectGroupsStreamLinkRequest_GroupIds:
		{
			dataset, err := endpoint.ReadHandler.GetDataset(uint(value.GroupIds.GetDatasetId()))
			if err != nil {
				log.Println(err.Error())
				return nil, err
			}

			projectID = dataset.ProjectID
		}
	case *services.GetObjectGroupsStreamLinkRequest_Dataset:
		{
			dataset, err := endpoint.ReadHandler.GetDataset(uint(value.Dataset.GetDatasetId()))
			if err != nil {
				log.Println(err.Error())
				return nil, err
			}

			projectID = dataset.ProjectID
		}
	case *services.GetObjectGroupsStreamLinkRequest_DatasetVersion:
		{
			dataset, err := endpoint.ReadHandler.GetDatasetVersion(uint(value.DatasetVersion.GetDatasetVersion()))
			if err != nil {
				log.Println(err.Error())
				return nil, err
			}

			projectID = dataset.ProjectID
		}
	default:
		return nil, status.Error(codes.Unauthenticated, "could not authorize requested action")
	}

	metadata, _ := metadata.FromIncomingContext(ctx)

	err := endpoint.AuthzHandler.Authorize(
		uint(projectID),
		protoModels.Right_READ,
		metadata)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	link, err := endpoint.ObjectStreamhandler.CreateStreamingLink(request, projectID)
	if err != nil {
		log.Println(err.Error())
		return nil, status.Error(codes.Internal, "could not create link")
	}

	response := &services.GetObjectGroupsStreamLinkResponse{
		Url: link,
	}

	return response, nil
}

// Updates a field of a dataset
func (endpoint *DatasetEndpoints) UpdateDatasetField(_ context.Context, _ *services.UpdateDatasetFieldRequest) (*services.UpdateDatasetFieldResponse, error) {
	return nil, status.Error(codes.Unimplemented, "unimplemented")
}

// DeleteDataset Delete a dataset
func (endpoint *DatasetEndpoints) DeleteDataset(ctx context.Context, request *services.DeleteDatasetRequest) (*services.DeleteDatasetResponse, error) {
	dataset, err := endpoint.ReadHandler.GetDataset(uint(request.GetId()))
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	metadata, _ := metadata.FromIncomingContext(ctx)

	err = endpoint.AuthzHandler.Authorize(
		uint(dataset.ProjectID),
		protoModels.Right_WRITE,
		metadata)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	objects, err := endpoint.ReadHandler.GetAllDatasetObjects(uint(request.GetId()))
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	err = endpoint.ObjectHandler.DeleteObjects(objects)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	err = endpoint.DeleteHandler.DeleteDataset(uint(request.GetId()))
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	return &services.DeleteDatasetResponse{}, nil
}

//ReleaseDatasetVersion Release a new dataset version
func (endpoint *DatasetEndpoints) ReleaseDatasetVersion(ctx context.Context, request *services.ReleaseDatasetVersionRequest) (*services.ReleaseDatasetVersionResponse, error) {
	dataset, err := endpoint.ReadHandler.GetDataset(uint(request.GetDatasetId()))
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	metadata, _ := metadata.FromIncomingContext(ctx)

	err = endpoint.AuthzHandler.Authorize(
		uint(dataset.ProjectID),
		protoModels.Right_WRITE,
		metadata)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	id, err := endpoint.CreateHandler.CreateDatasetVersion(request, dataset.ProjectID)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	response := &services.ReleaseDatasetVersionResponse{
		Id: uint64(id),
	}

	return response, nil
}

func (endpoint *DatasetEndpoints) GetDatasetVersion(ctx context.Context, request *services.GetDatasetVersionRequest) (*services.GetDatasetVersionResponse, error) {
	version, err := endpoint.ReadHandler.GetDatasetVersion(uint(request.GetId()))
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	metadata, _ := metadata.FromIncomingContext(ctx)

	err = endpoint.AuthzHandler.Authorize(
		uint(version.ProjectID),
		protoModels.Right_WRITE,
		metadata)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	protoVersion := version.ToProtoModel()

	response := &services.GetDatasetVersionResponse{
		DatasetVersion: protoVersion,
	}

	return response, nil
}

func (endpoint *DatasetEndpoints) GetDatasetVersionObjectGroups(ctx context.Context, request *services.GetDatasetVersionObjectGroupsRequest) (*services.GetDatasetVersionObjectGroupsResponse, error) {
	version, err := endpoint.ReadHandler.GetDatasetVersionWithObjectGroups(uint(request.GetId()))
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	metadata, _ := metadata.FromIncomingContext(ctx)

	err = endpoint.AuthzHandler.Authorize(
		uint(version.ProjectID),
		protoModels.Right_READ,
		metadata)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	var protoObjectGroups []*protoModels.ObjectGroup
	for _, objectGroup := range version.ObjectGroups {
		protoObjectGroups = append(protoObjectGroups, objectGroup.ToProtoModel())
	}

	response := &services.GetDatasetVersionObjectGroupsResponse{
		ObjectGroup: protoObjectGroups,
	}

	return response, nil
}

func (endpoint *DatasetEndpoints) DeleteDatasetVersion(ctx context.Context, request *services.DeleteDatasetVersionRequest) (*services.DeleteDatasetVersionResponse, error) {
	version, err := endpoint.ReadHandler.GetDatasetVersion(uint(request.GetId()))
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	metadata, _ := metadata.FromIncomingContext(ctx)

	err = endpoint.AuthzHandler.Authorize(
		uint(version.ProjectID),
		protoModels.Right_WRITE,
		metadata)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	err = endpoint.DeleteHandler.DeleteDatasetVersion(uint(request.GetId()))
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	return &services.DeleteDatasetVersionResponse{}, nil
}
