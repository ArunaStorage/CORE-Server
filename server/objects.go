package server

import (
	"context"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"

	"github.com/ScienceObjectsDB/CORE-Server/database"
	"github.com/ScienceObjectsDB/CORE-Server/models"
	v1notficationservices "github.com/ScienceObjectsDB/go-api/sciobjsdb/api/notification/services/v1"
	v1storagemodels "github.com/ScienceObjectsDB/go-api/sciobjsdb/api/storage/models/v1"
	v1storageservices "github.com/ScienceObjectsDB/go-api/sciobjsdb/api/storage/services/v1"
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
func (endpoint *ObjectServerEndpoints) CreateObjectGroup(ctx context.Context, request *v1storageservices.CreateObjectGroupRequest) (*v1storageservices.CreateObjectGroupResponse, error) {
	parsedDatasetID, err := uuid.Parse(request.DatasetId)
	if err != nil {
		log.Debug(err.Error())
		return nil, status.Error(codes.InvalidArgument, "could not parse dataset id")
	}

	dataset, err := endpoint.ReadHandler.GetDataset(parsedDatasetID)
	if err != nil {
		log.Errorln(err.Error())
		return nil, err
	}

	project, err := endpoint.ReadHandler.GetProject(dataset.ProjectID)
	if err != nil {
		log.Errorln(err.Error())
		return nil, err
	}

	metadata, _ := metadata.FromIncomingContext(ctx)

	err = endpoint.AuthzHandler.Authorize(
		dataset.ProjectID,
		v1storagemodels.Right_RIGHT_WRITE,
		metadata)
	if err != nil {
		log.Errorln(err.Error())
		return nil, err
	}

	objectgroup, err := endpoint.CreateHandler.CreateObjectGroup(request, dataset, project)
	if err != nil {
		log.Errorln(err.Error())
		return nil, err
	}

	objectGroupResponse := &v1storageservices.CreateObjectGroupResponse{
		ObjectGroupId:   objectgroup.ID.String(),
		ObjectGroupName: objectgroup.CurrentObjectGroupRevision.Name,
		CreateRevisionResponse: &v1storageservices.CreateObjectGroupRevisionResponse{
			Id: objectgroup.CurrentObjectGroupRevisionID.String(),
		},
	}

	err = endpoint.EventStreamMgmt.PublishMessage(&v1notficationservices.EventNotificationMessage{
		Resource:    v1storagemodels.Resource_RESOURCE_OBJECT_GROUP,
		ResourceId:  objectGroupResponse.ObjectGroupId,
		UpdatedType: v1notficationservices.EventNotificationMessage_UPDATE_TYPE_CREATED,
	})

	if err != nil {
		log.Errorln(err.Error())
		return nil, status.Error(codes.Internal, "could not publish notification event")
	}

	return objectGroupResponse, nil
}

func (endpoint *ObjectServerEndpoints) CreateObjectGroupBatch(ctx context.Context, requests *v1storageservices.CreateObjectGroupBatchRequest) (*v1storageservices.CreateObjectGroupBatchResponse, error) {
	if len(requests.GetRequests()) < 1 {
		return nil, status.Error(codes.InvalidArgument, "at least one request in request batch is required")
	}

	objects := make([]*database.RevisionObjects, 0)

	datasetID := requests.GetRequests()[0].DatasetId
	for _, request := range requests.GetRequests() {
		if datasetID != request.GetDatasetId() {
			return nil, status.Error(codes.InvalidArgument, "all requests have to have the same datasetid")
		}
		objects = append(objects, &database.RevisionObjects{
			DataObjects: &database.Objects{
				AddedObjects:    []models.Object{},
				UpdatedObjects:  []models.Object{},
				ExistingObjects: []models.Object{},
			},
			MetaObjects: &database.Objects{
				AddedObjects:    []models.Object{},
				UpdatedObjects:  []models.Object{},
				ExistingObjects: []models.Object{},
			},
		})
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
		v1storagemodels.Right_RIGHT_WRITE,
		metadata)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	objectgroups, err := endpoint.CreateHandler.CreateObjectGroupBatch(requests, dataset.Bucket, objects)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	var objectgroupResponseList []*v1storageservices.CreateObjectGroupResponse

	for _, objectgroup := range objectgroups {
		objectGroupResponse := &v1storageservices.CreateObjectGroupResponse{
			ObjectGroupId:   objectgroup.ID.String(),
			ObjectGroupName: objectgroup.CurrentObjectGroupRevision.Name,
			CreateRevisionResponse: &v1storageservices.CreateObjectGroupRevisionResponse{
				Id:                  objectgroup.CurrentObjectGroupRevisionID.String(),
				DataObjects:         []*v1storagemodels.Object{},
				MetaObjects:         []*v1storagemodels.Object{},
				ObjectLinks:         []*v1storageservices.CreateObjectGroupRevisionResponse_ObjectLinks{},
				MetadataObjectLinks: []*v1storageservices.CreateObjectGroupRevisionResponse_ObjectLinks{},
			},
		}

		if requests.IncludeObjectLink {
			objects, objectLinks, err := endpoint.createObjectsLinks(objectgroup.CurrentObjectGroupRevision.DataObjects)
			if err != nil {
				log.Errorln(err.Error())
				return nil, err
			}

			objectGroupResponse.CreateRevisionResponse.DataObjects = objects
			objectGroupResponse.CreateRevisionResponse.ObjectLinks = objectLinks

			metaObjects, metaObjectLinks, err := endpoint.createObjectsLinks(objectgroup.CurrentObjectGroupRevision.MetaObjects)
			if err != nil {
				log.Errorln(err.Error())
				return nil, err
			}

			objectGroupResponse.CreateRevisionResponse.MetaObjects = metaObjects
			objectGroupResponse.CreateRevisionResponse.MetadataObjectLinks = metaObjectLinks
		}

		objectgroupResponseList = append(objectgroupResponseList, objectGroupResponse)
	}

	response := &v1storageservices.CreateObjectGroupBatchResponse{
		Responses: objectgroupResponseList,
	}

	for _, createdObjectGroup := range objectgroupResponseList {
		err = endpoint.EventStreamMgmt.PublishMessage(&v1notficationservices.EventNotificationMessage{
			Resource:    v1storagemodels.Resource_RESOURCE_OBJECT_GROUP,
			ResourceId:  createdObjectGroup.GetObjectGroupId(),
			UpdatedType: v1notficationservices.EventNotificationMessage_UPDATE_TYPE_CREATED,
		})

		if err != nil {
			log.Println(err.Error())
			return nil, err
		}
	}

	return response, nil
}

//GetObjectGroup Returns the object group with the given ID
func (endpoint *ObjectServerEndpoints) GetObjectGroup(ctx context.Context, request *v1storageservices.GetObjectGroupRequest) (*v1storageservices.GetObjectGroupResponse, error) {
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
		v1storagemodels.Right_RIGHT_READ,
		metadata)
	if err != nil {
		log.Errorln(err.Error())
		return nil, err
	}

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
	response := v1storageservices.GetObjectGroupResponse{
		ObjectGroup: protoObjectGroup,
	}

	return &response, nil
}

func (endpoint *ObjectServerEndpoints) GetObjectGroupRevision(ctx context.Context, request *v1storageservices.GetObjectGroupRevisionRequest) (*v1storageservices.GetObjectGroupRevisionResponse, error) {
	requestID, err := uuid.Parse(request.GetId())
	if err != nil {
		log.Debug(err.Error())
		return nil, status.Error(codes.InvalidArgument, "could not parse submitted id")
	}

	objectGroupRevision, err := endpoint.ReadHandler.GetObjectGroupRevision(requestID)
	if err != nil {
		log.Errorln(err.Error())
		return nil, err
	}

	metadata, _ := metadata.FromIncomingContext(ctx)

	err = endpoint.AuthzHandler.Authorize(
		objectGroupRevision.ProjectID,
		v1storagemodels.Right_RIGHT_READ,
		metadata)
	if err != nil {
		log.Errorln(err.Error())
		return nil, err
	}

	objectGroupRevisionStats, err := endpoint.StatsHandler.GetObjectGroupRevisionStats(objectGroupRevision)
	if err != nil {
		log.Errorln(err.Error())
		return nil, status.Error(codes.Internal, "could not read stats for revision")
	}

	protoRevision, err := objectGroupRevision.ToProtoModel(objectGroupRevisionStats)
	if err != nil {
		log.Errorln(err.Error())
		return nil, err
	}

	response := &v1storageservices.GetObjectGroupRevisionResponse{
		ObjectGroupRevision: protoRevision,
	}

	return response, nil
}

// Updates an ObjectGroup
// This creates a new ObjectGroupRevisions
// It needs to be finished via FinishObjectGroupRevisionUpload before it is actually available
// Currently experimental
func (endpoint *ObjectServerEndpoints) UpdateObjectGroup(ctx context.Context, request *v1storageservices.UpdateObjectGroupRequest) (*v1storageservices.UpdateObjectGroupResponse, error) {
	requestID, err := uuid.Parse(request.GetId())
	if err != nil {
		log.Debug(err.Error())
		return nil, status.Error(codes.InvalidArgument, "could not parse submitted id")
	}

	objectGroup, err := endpoint.ReadHandler.GetObjectGroup(requestID)
	if err != nil {
		log.Errorln(err.Error())
		return nil, err
	}

	metadata, _ := metadata.FromIncomingContext(ctx)

	err = endpoint.AuthzHandler.Authorize(
		objectGroup.ProjectID,
		v1storagemodels.Right_RIGHT_WRITE,
		metadata)
	if err != nil {
		log.Errorln(err.Error())
		return nil, err
	}

	objectGroupRevision, err := endpoint.UpdateHandler.UpdateObjectGroup(request, &objectGroup.Dataset, &objectGroup.Project, objectGroup)
	if err != nil {
		log.Errorln(err.Error())
		return nil, err
	}

	msg := &v1notficationservices.EventNotificationMessage{Resource: v1storagemodels.Resource_RESOURCE_OBJECT_GROUP, ResourceId: objectGroupRevision.ObjectGroupID.String(), UpdatedType: v1notficationservices.EventNotificationMessage_UPDATE_TYPE_UPDATED}

	err = endpoint.EventStreamMgmt.PublishMessage(msg)
	if err != nil {
		log.Errorln(err.Error())
		return nil, err
	}

	return &v1storageservices.UpdateObjectGroupResponse{}, nil
}

//FinishObjectUpload Finishes the upload process for an object
func (endpoint *ObjectServerEndpoints) FinishObjectUpload(ctx context.Context, request *v1storageservices.FinishObjectUploadRequest) (*v1storageservices.FinishObjectUploadResponse, error) {
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
		v1storagemodels.Right_RIGHT_WRITE,
		metadata)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	finished := &v1storageservices.FinishObjectUploadResponse{}

	return finished, nil
}

//FinishObjectGroupRevisionUpload Finishes the upload process for an object
func (endpoint *ObjectServerEndpoints) FinishObjectGroupRevisionUpload(ctx context.Context, request *v1storageservices.FinishObjectGroupRevisionUploadRequest) (*v1storageservices.FinishObjectGroupRevisionUploadResponse, error) {
	requestID, err := uuid.Parse(request.GetId())
	if err != nil {
		log.Debug(err.Error())
		return nil, status.Error(codes.InvalidArgument, "could not parse submitted id")
	}

	objectGroupRevision, err := endpoint.ReadHandler.GetObjectGroupRevision(requestID)
	if err != nil {
		log.Errorln(err.Error())
		return nil, err
	}

	metadata, _ := metadata.FromIncomingContext(ctx)

	err = endpoint.AuthzHandler.Authorize(
		objectGroupRevision.ProjectID,
		v1storagemodels.Right_RIGHT_WRITE,
		metadata)
	if err != nil {
		log.Errorln(err.Error())
		return nil, err
	}

	err = endpoint.UpdateHandler.FinishObjectGroupRevisionUpload(requestID)
	if err != nil {
		log.Errorln(err.Error())
		return nil, status.Error(codes.Internal, "could not finish objectgroup revision")
	}

	msg := &v1notficationservices.EventNotificationMessage{Resource: v1storagemodels.Resource_RESOURCE_OBJECT_GROUP, ResourceId: objectGroupRevision.ObjectGroupID.String(), UpdatedType: v1notficationservices.EventNotificationMessage_UPDATE_TYPE_AVAILABLE}

	err = endpoint.EventStreamMgmt.PublishMessage(msg)
	if err != nil {
		log.Errorln(err.Error())
		return nil, err
	}

	finished := &v1storageservices.FinishObjectGroupRevisionUploadResponse{}

	return finished, nil
}

func (endpoint *ObjectServerEndpoints) DeleteObjectGroup(ctx context.Context, request *v1storageservices.DeleteObjectGroupRequest) (*v1storageservices.DeleteObjectGroupResponse, error) {
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
		v1storagemodels.Right_RIGHT_WRITE,
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

	var locations []*models.Location
	for _, object := range objects {
		locations = append(locations, &object.DefaultLocation)
	}

	if len(objects) != 0 {
		err = endpoint.ObjectHandler.DeleteObjects(locations)
		if err != nil {
			log.Println(err.Error())
			return nil, err
		}
	}

	err = endpoint.EventStreamMgmt.PublishMessage(&v1notficationservices.EventNotificationMessage{
		Resource:    v1storagemodels.Resource_RESOURCE_OBJECT_GROUP,
		ResourceId:  request.GetId(),
		UpdatedType: v1notficationservices.EventNotificationMessage_UPDATE_TYPE_DELETED,
	})

	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	err = endpoint.DeleteHandler.DeleteObjectGroup(requestID)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	return &v1storageservices.DeleteObjectGroupResponse{}, nil
}

func (endpoint *Endpoints) createObjectsLinks(objects []models.Object) ([]*v1storagemodels.Object, []*v1storageservices.CreateObjectGroupRevisionResponse_ObjectLinks, error) {
	var protoObjects []*v1storagemodels.Object
	var objectLinks []*v1storageservices.CreateObjectGroupRevisionResponse_ObjectLinks

	for _, object := range objects {
		protoObject, err := object.ToProtoModel()
		if err != nil {
			log.Errorln(err.Error())
			return nil, nil, err
		}

		protoObjects = append(protoObjects, protoObject)

		if protoObject.Status == v1storagemodels.Status_STATUS_STAGING {
			link, err := endpoint.ObjectHandler.CreateUploadLink(&object.DefaultLocation)
			if err != nil {
				log.Errorln(err.Error())
				return nil, nil, err
			}
			objectLinks = append(objectLinks, &v1storageservices.CreateObjectGroupRevisionResponse_ObjectLinks{
				Link:     link,
				ObjectId: object.ID.String(),
			})
		} else {
			objectLinks = append(objectLinks, nil)
		}
	}

	return protoObjects, objectLinks, nil
}

func (endpoint *Endpoints) CreateObject(ctx context.Context, request *v1storageservices.CreateObjectRequest) (*v1storageservices.CreateObjectResponse, error) {
	datasetUUID, err := uuid.Parse(request.DatasetId)
	if err != nil {
		log.Errorln(err.Error())
		return nil, status.Error(codes.InvalidArgument, "could not parse provided dataset id, expected a valid UUID")
	}

	dataset, err := endpoint.ReadHandler.GetDataset(datasetUUID)
	if err != nil {
		log.Errorln(err.Error())
		return nil, status.Error(codes.Internal, "could not read request dataset")
	}

	project, err := endpoint.ReadHandler.GetProject(dataset.ProjectID)
	if err != nil {
		log.Errorln(err.Error())
		return nil, status.Error(codes.Internal, "could not read request project")
	}

	metadata, _ := metadata.FromIncomingContext(ctx)

	err = endpoint.AuthzHandler.Authorize(
		project.ID,
		v1storagemodels.Right_RIGHT_WRITE,
		metadata)
	if err != nil {
		log.Errorln(err.Error())
		return nil, err
	}

	object, err := endpoint.CreateHandler.CreateObject(request, project, dataset)
	if err != nil {
		log.Errorln(err.Error())
		return nil, status.Error(codes.Internal, "could not create requested object")
	}

	link, err := endpoint.ObjectHandler.CreateUploadLink(&object.DefaultLocation)
	if err != nil {
		log.Errorln(err.Error())
		return nil, status.Error(codes.Internal, "coult not create upload link for object")
	}

	return &v1storageservices.CreateObjectResponse{Id: object.ID.String(), UploadLink: link}, nil
}
