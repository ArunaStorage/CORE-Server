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

	id, revision_id, err := endpoint.CreateHandler.CreateObjectGroup(request)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	response := services.CreateObjectGroupResponse{
		ObjectGroupId: uint64(id),
		RevisionId:    uint64(revision_id),
	}

	return &response, nil
}

//CreateObjectGroupVersion Creates a new object group version
func (endpoint *ObjectServerEndpoints) AddRevisionToObjectGroup(ctx context.Context, request *services.AddRevisionToObjectGroupRequest) (*services.AddRevisionToObjectGroupResponse, error) {
	objectGroup, err := endpoint.ReadHandler.GetObjectGroup(uint(request.GetObjectGroupId()))
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

	id, err := endpoint.CreateHandler.AddObjectGroupRevision(request)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	objectgroupRevision, err := endpoint.ReadHandler.GetObjectGroupRevision(id)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	response := &services.AddRevisionToObjectGroupResponse{
		RevisionId:     request.GetObjectGroupId(),
		RevisionNumber: objectgroupRevision.Revision,
	}

	return response, nil
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

//GetObjectGroupCurrentVersion Returns the head version in the history of a given object group
func (endpoint *ObjectServerEndpoints) GetCurrentObjectGroupRevision(ctx context.Context, request *services.GetCurrentObjectGroupRevisionRequest) (*services.GetCurrentObjectGroupRevisionResponse, error) {
	revision, err := endpoint.ReadHandler.GetCurrentObjectGroupRevision(uint(request.GetId()))
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	metadata, _ := metadata.FromIncomingContext(ctx)

	err = endpoint.AuthzHandler.Authorize(
		revision.ProjectID,
		protoModels.Right_READ,
		metadata)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	protoRevision := revision.ToProtoModel()

	response := services.GetCurrentObjectGroupRevisionResponse{
		ObjectGroupRevision: protoRevision,
	}

	return &response, nil
}

func (endpoint *ObjectServerEndpoints) GetObjectGroupRevision(ctx context.Context, request *services.GetObjectGroupRevisionRequest) (*services.GetObjectGroupRevisionResponse, error) {
	revision, err := endpoint.ReadHandler.GetObjectGroupRevision(uint(request.Id))
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	metadata, _ := metadata.FromIncomingContext(ctx)

	err = endpoint.AuthzHandler.Authorize(
		revision.ProjectID,
		protoModels.Right_READ,
		metadata)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	protoRevision := revision.ToProtoModel()
	response := services.GetObjectGroupRevisionResponse{
		ObjectGroupRevision: protoRevision,
	}

	return &response, nil
}

func (endpoint *ObjectServerEndpoints) GetObjectGroupRevisions(ctx context.Context, request *services.GetObjectGroupRevisionsRequest) (*services.GetObjectGroupRevisionsResponse, error) {
	objectGroup, err := endpoint.ReadHandler.GetObjectGroup(uint(request.Id))
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

	revisions, err := endpoint.ReadHandler.GetObjectGroupRevisions(uint(request.GetId()))
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	var protoRevisions []*protoModels.ObjectGroupRevision
	for _, revision := range revisions {
		protoRevision := revision.ToProtoModel()
		protoRevisions = append(protoRevisions, protoRevision)
	}

	response := &services.GetObjectGroupRevisionsResponse{
		ObjectGroupRevision: protoRevisions,
	}

	return response, nil
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

func (endpoint *ObjectServerEndpoints) DeleteObjectGroupRevision(ctx context.Context, request *services.DeleteObjectGroupRevisionRequest) (*services.DeleteObjectGroupRevisionResponse, error) {
	revision, err := endpoint.ReadHandler.GetCurrentObjectGroupRevision(uint(request.GetId()))
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	metadata, _ := metadata.FromIncomingContext(ctx)

	err = endpoint.AuthzHandler.Authorize(
		revision.ProjectID,
		protoModels.Right_WRITE,
		metadata)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	objects, err := endpoint.ReadHandler.GetAllObjectGroupRevisionObjects(uint(request.GetId()))
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	err = endpoint.ObjectHandler.DeleteObjects(objects)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	err = endpoint.DeleteHandler.DeleteObjectGroupRevision(uint(request.GetId()))
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	return &services.DeleteObjectGroupRevisionResponse{}, nil
}
