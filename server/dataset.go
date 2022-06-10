package server

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/ScienceObjectsDB/CORE-Server/models"
	v1notificationservices "github.com/ScienceObjectsDB/go-api/sciobjsdb/api/notification/services/v1"
	v1storagemodels "github.com/ScienceObjectsDB/go-api/sciobjsdb/api/storage/models/v1"
	v1storageservices "github.com/ScienceObjectsDB/go-api/sciobjsdb/api/storage/services/v1"
)

type DatasetEndpoints struct {
	*Endpoints
}

//NewDatasetEndpoints New dataset service
func NewDatasetEndpoints(endpoints *Endpoints) (*DatasetEndpoints, error) {
	datasetEndpoint := &DatasetEndpoints{
		Endpoints: endpoints,
	}

	return datasetEndpoint, nil
}

// CreateNewDataset Creates a new dataset and associates it with a dataset
func (endpoint *DatasetEndpoints) CreateDataset(ctx context.Context, request *v1storageservices.CreateDatasetRequest) (*v1storageservices.CreateDatasetResponse, error) {
	projectID, err := uuid.Parse(request.GetProjectId())
	if err != nil {
		log.Debug(err.Error())
		return nil, status.Error(codes.InvalidArgument, "could not parse dataset id")
	}

	metadata, _ := metadata.FromIncomingContext(ctx)

	err = endpoint.AuthzHandler.Authorize(
		projectID,
		v1storagemodels.Right_RIGHT_WRITE,
		metadata)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	id, err := endpoint.CreateHandler.CreateDataset(request)
	if err != nil {
		log.Error(err.Error())
		return nil, err
	}

	msg := &v1notificationservices.EventNotificationMessage{
		ResourceId:  id,
		Resource:    v1storagemodels.Resource_RESOURCE_DATASET,
		UpdatedType: v1notificationservices.EventNotificationMessage_UPDATE_TYPE_CREATED,
	}

	err = endpoint.EventStreamMgmt.PublishMessage(msg)
	if err != nil {
		log.Errorln(err.Error())
		return nil, status.Error(codes.Internal, "could not publish notification event")
	}

	response := v1storageservices.CreateDatasetResponse{
		Id: id,
	}

	return &response, nil
}

// Dataset Returns a specific dataset
func (endpoint *DatasetEndpoints) GetDataset(ctx context.Context, request *v1storageservices.GetDatasetRequest) (*v1storageservices.GetDatasetResponse, error) {
	requestID, err := uuid.Parse(request.GetId())
	if err != nil {
		log.Debug(err.Error())
		return nil, status.Error(codes.InvalidArgument, "could not parse dataset id")
	}

	dataset, err := endpoint.ReadHandler.GetDataset(requestID)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	metadata, _ := metadata.FromIncomingContext(ctx)

	err = endpoint.AuthzHandler.Authorize(
		dataset.ProjectID,
		v1storagemodels.Right_RIGHT_READ,
		metadata)
	if err != nil {
		log.Errorln(err.Error())
		return nil, err
	}

	stats, err := endpoint.StatsHandler.GetDatasetStats(dataset.ID)
	if err != nil {
		log.Errorln(err.Error())
		return nil, status.Error(codes.Internal, "error while reading dataset statistics")
	}

	protoDataset, err := dataset.ToProtoModel(stats)
	if err != nil {
		log.Errorln(err.Error())
		return nil, status.Error(codes.Internal, "could not create dataset protobuf representation")
	}
	response := v1storageservices.GetDatasetResponse{
		Dataset: protoDataset,
	}

	return &response, nil
}

func (endpoint *DatasetEndpoints) GetDatasetObjects(ctx context.Context, request *v1storageservices.GetDatasetObjectsRequest) (*v1storageservices.GetDatasetObjectsResponse, error) {
	requestID, err := uuid.Parse(request.GetId())
	if err != nil {
		log.Debug(err.Error())
		return nil, status.Error(codes.InvalidArgument, "could not parse dataset id")
	}

	dataset, err := endpoint.ReadHandler.GetDataset(requestID)
	if err != nil {
		log.Errorln(err.Error())
		return nil, status.Error(codes.Internal, "could not read dataset")
	}

	metadata, _ := metadata.FromIncomingContext(ctx)

	err = endpoint.AuthzHandler.Authorize(
		dataset.ProjectID,
		v1storagemodels.Right_RIGHT_READ,
		metadata)
	if err != nil {
		log.Errorln(err.Error())
		return nil, err
	}

	objects, err := endpoint.ReadHandler.GetDatasetObjects(request)
	if err != nil {
		log.Errorln(err.Error())
		return nil, err
	}

	var protoObjects []*v1storagemodels.Object
	for _, object := range objects {
		protoObject, err := object.ToProtoModel()
		if err != nil {
			log.Errorln(err.Error())
			return nil, status.Error(codes.Internal, "could not transform model objects to proto objects")
		}

		protoObjects = append(protoObjects, protoObject)
	}

	response := &v1storageservices.GetDatasetObjectsResponse{
		Objects: protoObjects,
	}

	return response, nil
}

// Lists Versions of a dataset
func (endpoint *DatasetEndpoints) GetDatasetVersions(ctx context.Context, request *v1storageservices.GetDatasetVersionsRequest) (*v1storageservices.GetDatasetVersionsResponse, error) {
	requestID, err := uuid.Parse(request.GetId())
	if err != nil {
		log.Debug(err.Error())
		return nil, status.Error(codes.InvalidArgument, "could not parse dataset id")
	}

	dataset, err := endpoint.ReadHandler.GetDataset(requestID)
	if err != nil {
		log.Errorln(err.Error())
		return nil, err
	}

	metadata, _ := metadata.FromIncomingContext(ctx)

	err = endpoint.AuthzHandler.Authorize(
		dataset.ProjectID,
		v1storagemodels.Right_RIGHT_READ,
		metadata)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	versions, err := endpoint.ReadHandler.GetDatasetVersions(requestID)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	var protoVersions []*v1storagemodels.DatasetVersion
	for _, version := range versions {
		versionStats, err := endpoint.StatsHandler.GetDatasetVersionStats(&version)
		if err != nil {
			log.Errorln(err.Error())
			return nil, status.Error(codes.Internal, "could not read dataset version statistics from the database")
		}

		protoVersion, err := version.ToProtoModel(versionStats)
		if err != nil {
			log.Errorln(err.Error())
			return nil, status.Error(codes.Internal, "could not transform datasetversion into protobuf representation")
		}

		protoVersions = append(protoVersions, protoVersion)
	}

	response := &v1storageservices.GetDatasetVersionsResponse{
		DatasetVersions: protoVersions,
	}

	return response, nil
}

func (endpoint *DatasetEndpoints) GetDatasetObjectGroups(ctx context.Context, request *v1storageservices.GetDatasetObjectGroupsRequest) (*v1storageservices.GetDatasetObjectGroupsResponse, error) {
	requestID, err := uuid.Parse(request.GetId())
	if err != nil {
		log.Debug(err.Error())
		return nil, status.Error(codes.InvalidArgument, "could not parse dataset id")
	}

	dataset, err := endpoint.ReadHandler.GetDataset(requestID)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	metadata, _ := metadata.FromIncomingContext(ctx)

	err = endpoint.AuthzHandler.Authorize(
		dataset.ProjectID,
		v1storagemodels.Right_RIGHT_READ,
		metadata)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	objectGroups, err := endpoint.ReadHandler.GetDatasetObjectGroups(requestID, request.GetPageRequest())
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	var protoObjectGroups []*v1storagemodels.ObjectGroup
	for _, objectGroup := range objectGroups {
		stats, err := endpoint.StatsHandler.GetObjectGroupRevisionStats(&objectGroup.CurrentObjectGroupRevision)
		if err != nil {
			log.Errorln(err.Error())
			return nil, err
		}

		protoObjectGroup, err := objectGroup.ToProtoModel(stats)
		if err != nil {
			log.Errorln(err.Error())
			return nil, status.Error(codes.Internal, "could not transform objectgroup into protobuf representation")
		}
		protoObjectGroups = append(protoObjectGroups, protoObjectGroup)
	}

	response := v1storageservices.GetDatasetObjectGroupsResponse{
		ObjectGroups: protoObjectGroups,
	}

	return &response, nil
}

func (endpoint *DatasetEndpoints) GetObjectGroupRevisionsInDateRange(ctx context.Context, request *v1storageservices.GetObjectGroupRevisionsInDateRangeRequest) (*v1storageservices.GetObjectGroupRevisionsInDateRangeResponse, error) {
	requestID, err := uuid.Parse(request.GetId())
	if err != nil {
		log.Debug(err.Error())
		return nil, status.Error(codes.InvalidArgument, "could not parse dataset id")
	}

	dataset, err := endpoint.ReadHandler.GetDataset(requestID)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	metadata, _ := metadata.FromIncomingContext(ctx)

	err = endpoint.AuthzHandler.Authorize(
		dataset.ProjectID,
		v1storagemodels.Right_RIGHT_READ,
		metadata)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	objectGroupRevisions, err := endpoint.ReadHandler.GetObjectGroupRevisionsInDateRange(dataset.ID, request.Start.AsTime(), request.End.AsTime())
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	var protoObjectGroups []*v1storagemodels.ObjectGroupRevision
	for _, objectGroupRevision := range objectGroupRevisions {
		stats, err := endpoint.StatsHandler.GetObjectGroupRevisionStats(objectGroupRevision)
		if err != nil {
			log.Errorln(err.Error())
			return nil, err
		}

		protoObjectGroup, err := objectGroupRevision.ToProtoModel(stats)
		if err != nil {
			log.Errorln(err.Error())
			return nil, status.Error(codes.Internal, "could not transform objectgroup into protobuf representation")
		}
		protoObjectGroups = append(protoObjectGroups, protoObjectGroup)
	}

	response := &v1storageservices.GetObjectGroupRevisionsInDateRangeResponse{
		ObjectGroupRevisions: protoObjectGroups,
	}

	return response, nil
}

func (endpoint *DatasetEndpoints) GetObjectGroupsStreamLink(ctx context.Context, request *v1storageservices.GetObjectGroupsStreamLinkRequest) (*v1storageservices.GetObjectGroupsStreamLinkResponse, error) {
	var projectID uuid.UUID

	switch value := request.Query.(type) {
	case *v1storageservices.GetObjectGroupsStreamLinkRequest_GroupIds:
		{
			datasetID, err := uuid.Parse(value.GroupIds.GetDatasetId())
			if err != nil {
				log.Debug(err.Error())
				return nil, status.Error(codes.InvalidArgument, "could not parse dataset id")
			}

			dataset, err := endpoint.ReadHandler.GetDataset(datasetID)
			if err != nil {
				log.Println(err.Error())
				return nil, err
			}

			projectID = dataset.ProjectID
		}
	case *v1storageservices.GetObjectGroupsStreamLinkRequest_Dataset:
		{
			datasetID, err := uuid.Parse(value.Dataset.GetDatasetId())
			if err != nil {
				log.Debug(err.Error())
				return nil, status.Error(codes.InvalidArgument, "could not parse dataset id")
			}

			dataset, err := endpoint.ReadHandler.GetDataset(datasetID)
			if err != nil {
				log.Println(err.Error())
				return nil, err
			}

			projectID = dataset.ProjectID
		}
	case *v1storageservices.GetObjectGroupsStreamLinkRequest_DatasetVersion:
		{
			datasetVersionID, err := uuid.Parse(value.DatasetVersion.GetDatasetVersionId())
			if err != nil {
				log.Debug(err.Error())
				return nil, status.Error(codes.InvalidArgument, "could not parse dataset id")
			}

			dataset, err := endpoint.ReadHandler.GetDatasetVersion(datasetVersionID)
			if err != nil {
				log.Println(err.Error())
				return nil, err
			}

			projectID = dataset.ProjectID
		}
	case *v1storageservices.GetObjectGroupsStreamLinkRequest_DateRange:
		{
			datasetID, err := uuid.Parse(value.DateRange.GetDatasetId())
			if err != nil {
				log.Debug(err.Error())
				return nil, status.Error(codes.InvalidArgument, "could not parse dataset id")
			}

			dataset, err := endpoint.ReadHandler.GetDataset(datasetID)
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
		projectID,
		v1storagemodels.Right_RIGHT_READ,
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

	response := &v1storageservices.GetObjectGroupsStreamLinkResponse{
		Url: link,
	}

	return response, nil
}

// Updates a field of a dataset
func (endpoint *DatasetEndpoints) UpdateDatasetField(_ context.Context, _ *v1storageservices.UpdateDatasetFieldRequest) (*v1storageservices.UpdateDatasetFieldResponse, error) {
	return nil, status.Error(codes.Unimplemented, "unimplemented")
}

// DeleteDataset Delete a dataset
func (endpoint *DatasetEndpoints) DeleteDataset(ctx context.Context, request *v1storageservices.DeleteDatasetRequest) (*v1storageservices.DeleteDatasetResponse, error) {
	requestID, err := uuid.Parse(request.GetId())
	if err != nil {
		log.Debug(err.Error())
		return nil, status.Error(codes.InvalidArgument, "could not parse id")
	}

	dataset, err := endpoint.ReadHandler.GetDataset(requestID)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	metadata, _ := metadata.FromIncomingContext(ctx)

	err = endpoint.AuthzHandler.Authorize(
		dataset.ProjectID,
		v1storagemodels.Right_RIGHT_WRITE,
		metadata)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	objects, err := endpoint.ReadHandler.GetAllDatasetObjects(requestID)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	var locations []*models.Location
	for _, location := range objects {
		locations = append(locations, &location.DefaultLocation)
	}

	err = endpoint.ObjectHandler.DeleteObjects(locations)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	msg := &v1notificationservices.EventNotificationMessage{
		ResourceId:  dataset.ID.String(),
		Resource:    v1storagemodels.Resource_RESOURCE_DATASET,
		UpdatedType: v1notificationservices.EventNotificationMessage_UPDATE_TYPE_DELETED,
	}
	err = endpoint.EventStreamMgmt.PublishMessage(msg)
	if err != nil {
		log.Errorln(err.Error())
		return nil, status.Error(codes.Internal, "could not publish notification event")
	}

	err = endpoint.DeleteHandler.DeleteDataset(requestID)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	return &v1storageservices.DeleteDatasetResponse{}, nil
}

//ReleaseDatasetVersion Release a new dataset version
func (endpoint *DatasetEndpoints) ReleaseDatasetVersion(ctx context.Context, request *v1storageservices.ReleaseDatasetVersionRequest) (*v1storageservices.ReleaseDatasetVersionResponse, error) {
	datasetID, err := uuid.Parse(request.GetDatasetId())
	if err != nil {
		log.Debug(err.Error())
		return nil, status.Error(codes.InvalidArgument, "could not parse id")
	}

	dataset, err := endpoint.ReadHandler.GetDataset(datasetID)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	metadata, _ := metadata.FromIncomingContext(ctx)

	err = endpoint.AuthzHandler.Authorize(
		dataset.ProjectID,
		v1storagemodels.Right_RIGHT_WRITE,
		metadata)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	objectGroupRevisions, err := endpoint.ReadHandler.GetObjectGroupRevisionsByStatus(request.ObjectGroupRevisionIds, []string{v1storagemodels.Status_STATUS_AVAILABLE.String()})
	if err != nil {
		log.Errorln(err.Error())
		return nil, status.Error(codes.Internal, "error when reading the requested object groups")
	}

	if len(objectGroupRevisions) != 0 {
		var nonAvailableObjectGroupIDs []string
		for _, objectGroup := range objectGroupRevisions {
			nonAvailableObjectGroupIDs = append(nonAvailableObjectGroupIDs, objectGroup.ID.String())
		}
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("the following object groups are not in available status: %v", nonAvailableObjectGroupIDs))
	}

	id, err := endpoint.CreateHandler.CreateDatasetVersion(request, dataset.ProjectID)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	response := &v1storageservices.ReleaseDatasetVersionResponse{
		Id: id.String(),
	}

	msg := &v1notificationservices.EventNotificationMessage{
		ResourceId:  id.String(),
		Resource:    v1storagemodels.Resource_RESOURCE_DATASET_VERSION,
		UpdatedType: v1notificationservices.EventNotificationMessage_UPDATE_TYPE_CREATED,
	}
	err = endpoint.EventStreamMgmt.PublishMessage(msg)
	if err != nil {
		log.Errorln(err.Error())
		return nil, status.Error(codes.Internal, "could not publish notification event")
	}

	return response, nil
}

func (endpoint *DatasetEndpoints) GetDatasetVersion(ctx context.Context, request *v1storageservices.GetDatasetVersionRequest) (*v1storageservices.GetDatasetVersionResponse, error) {
	requestID, err := uuid.Parse(request.GetId())
	if err != nil {
		log.Debug(err.Error())
		return nil, status.Error(codes.InvalidArgument, "could not parse dataset id")
	}

	version, err := endpoint.ReadHandler.GetDatasetVersion(requestID)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	metadata, _ := metadata.FromIncomingContext(ctx)

	err = endpoint.AuthzHandler.Authorize(
		version.ProjectID,
		v1storagemodels.Right_RIGHT_WRITE,
		metadata)
	if err != nil {
		log.Errorln(err.Error())
		return nil, err
	}

	versionStats, err := endpoint.StatsHandler.GetDatasetVersionStats(version)
	if err != nil {
		log.Errorln(err.Error())
		return nil, status.Error(codes.Internal, "could not read dataset version statistics from the database")
	}

	protoVersion, err := version.ToProtoModel(versionStats)
	if err != nil {
		log.Errorln(err.Error())
		return nil, status.Error(codes.Internal, "could not transform datasetversion into protobuf representation")
	}

	response := &v1storageservices.GetDatasetVersionResponse{
		DatasetVersion: protoVersion,
	}

	return response, nil
}

func (endpoint *DatasetEndpoints) GetDatasetVersionObjectGroups(ctx context.Context, request *v1storageservices.GetDatasetVersionObjectGroupsRequest) (*v1storageservices.GetDatasetVersionObjectGroupsResponse, error) {
	requestID, err := uuid.Parse(request.GetId())
	if err != nil {
		log.Debug(err.Error())
		return nil, status.Error(codes.InvalidArgument, "could not parse dataset id")
	}

	version, err := endpoint.ReadHandler.GetDatasetVersionWithObjectGroups(requestID, request.GetPageRequest())
	if err != nil {
		log.Errorln(err.Error())
		return nil, err
	}

	metadata, _ := metadata.FromIncomingContext(ctx)

	err = endpoint.AuthzHandler.Authorize(
		version.ProjectID,
		v1storagemodels.Right_RIGHT_READ,
		metadata)
	if err != nil {
		log.Errorln(err.Error())
		return nil, err
	}

	var protoObjectGroupRevisions []*v1storagemodels.ObjectGroupRevision
	for _, objectGroupRevision := range version.ObjectGroupRevisions {
		stats, err := endpoint.StatsHandler.GetObjectGroupRevisionStats(&objectGroupRevision)
		if err != nil {
			log.Errorln(err.Error())
			return nil, status.Error(codes.Internal, "could not read objectgroup stats")
		}

		protoObjectGroupRevision, err := objectGroupRevision.ToProtoModel(stats)
		if err != nil {
			log.Errorln(err.Error())
			return nil, status.Error(codes.Internal, "could not transform objectgroup into protobuf representation")
		}
		protoObjectGroupRevisions = append(protoObjectGroupRevisions, protoObjectGroupRevision)
	}

	response := &v1storageservices.GetDatasetVersionObjectGroupsResponse{
		ObjectGroupRevisions: protoObjectGroupRevisions,
	}

	return response, nil
}

func (endpoint *DatasetEndpoints) DeleteDatasetVersion(ctx context.Context, request *v1storageservices.DeleteDatasetVersionRequest) (*v1storageservices.DeleteDatasetVersionResponse, error) {
	requestID, err := uuid.Parse(request.GetId())
	if err != nil {
		log.Debug(err.Error())
		return nil, status.Error(codes.InvalidArgument, "could not parse dataset id")
	}

	version, err := endpoint.ReadHandler.GetDatasetVersion(requestID)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	metadata, _ := metadata.FromIncomingContext(ctx)

	err = endpoint.AuthzHandler.Authorize(
		version.ProjectID,
		v1storagemodels.Right_RIGHT_WRITE,
		metadata)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	msg := &v1notificationservices.EventNotificationMessage{
		ResourceId:  version.ID.String(),
		Resource:    v1storagemodels.Resource_RESOURCE_DATASET,
		UpdatedType: v1notificationservices.EventNotificationMessage_UPDATE_TYPE_DELETED,
	}
	err = endpoint.EventStreamMgmt.PublishMessage(msg)
	if err != nil {
		log.Errorln(err.Error())
		return nil, status.Error(codes.Internal, "could not publish notification event")
	}

	err = endpoint.DeleteHandler.DeleteDatasetVersion(requestID)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	return &v1storageservices.DeleteDatasetVersionResponse{}, nil
}
