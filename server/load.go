package server

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	log "github.com/sirupsen/logrus"

	protoModels "github.com/ScienceObjectsDB/go-api/api/models/v1"
	services "github.com/ScienceObjectsDB/go-api/api/services/v1"
	"google.golang.org/grpc/metadata"
)

type LoadEndpoints struct {
	*Endpoints
}

// NewLoadEndpoints New load service
func NewLoadEndpoints(endpoints *Endpoints) (*LoadEndpoints, error) {
	loadEndpoint := &LoadEndpoints{
		Endpoints: endpoints,
	}

	return loadEndpoint, nil
}

func (endpoint *LoadEndpoints) CreateUploadLink(ctx context.Context, request *services.CreateUploadLinkRequest) (*services.CreateUploadLinkResponse, error) {
	object, err := endpoint.ReadHandler.GetObject(uint(request.GetId()))
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	metadata, _ := metadata.FromIncomingContext(ctx)

	err = endpoint.AuthzHandler.Authorize(
		object.ProjectID,
		protoModels.Right_READ,
		metadata)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	uploadLink, err := endpoint.ObjectHandler.CreateUploadLink(object)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	response := services.CreateUploadLinkResponse{
		UploadLink: uploadLink,
	}

	return &response, nil
}

func (endpoint *LoadEndpoints) CreateDownloadLink(ctx context.Context, request *services.CreateDownloadLinkRequest) (*services.CreateDownloadLinkResponse, error) {
	object, err := endpoint.ReadHandler.GetObject(uint(request.GetId()))
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	metadata, _ := metadata.FromIncomingContext(ctx)

	err = endpoint.AuthzHandler.Authorize(
		object.ProjectID,
		protoModels.Right_READ,
		metadata)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	downloadLink, err := endpoint.ObjectHandler.CreateDownloadLink(object)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	response := services.CreateDownloadLinkResponse{
		DownloadLink: downloadLink,
	}

	return &response, nil
}

func (endpoint *LoadEndpoints) CreateDownloadLinkBatch(ctx context.Context, request *services.CreateDownloadLinkBatchRequest) (*services.CreateDownloadLinkBatchResponse, error) {
	metadata, _ := metadata.FromIncomingContext(ctx)
	dlLinks := make([]*services.CreateDownloadLinkResponse, len(request.GetRequests()))
	projectIDs := make(map[uint]interface{})
	objectIDs := make([]uint, len(request.GetRequests()))
	for i, request := range request.GetRequests() {
		objectIDs[i] = uint(request.GetId())
	}

	objects, err := endpoint.ReadHandler.GetObjectsBatch(objectIDs)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	for _, object := range objects {
		projectIDs[object.ProjectID] = struct{}{}
	}

	for projectID := range projectIDs {
		err := endpoint.AuthzHandler.Authorize(projectID, protoModels.Right_READ, metadata)
		if err != nil {
			log.Println(err.Error())
			return nil, err
		}
	}

	for i, object := range objects {
		link, err := endpoint.ObjectHandler.CreateDownloadLink(object)
		if err != nil {
			log.Println(err.Error())
			return nil, err
		}

		dlLinks[i] = &services.CreateDownloadLinkResponse{
			DownloadLink: link,
			Object:       object.ToProtoModel(),
		}
	}

	response := &services.CreateDownloadLinkBatchResponse{
		Links: dlLinks,
	}

	return response, nil
}

func (endpoint *LoadEndpoints) StartMultipartUpload(ctx context.Context, request *services.StartMultipartUploadRequest) (*services.StartMultipartUploadResponse, error) {
	object, err := endpoint.ReadHandler.GetObject(uint(request.GetId()))
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

	uploadID, err := endpoint.ObjectHandler.InitMultipartUpload(object)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	err = endpoint.UpdateHandler.AddUploadID(object.ID, uploadID)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}
	object.UploadID = uploadID

	response := &services.StartMultipartUploadResponse{
		Object: object.ToProtoModel(),
	}

	return response, nil
}

func (endpoint *LoadEndpoints) GetMultipartUploadLink(ctx context.Context, request *services.GetMultipartUploadLinkRequest) (*services.GetMultipartUploadLinkResponse, error) {
	object, err := endpoint.ReadHandler.GetObject(uint(request.GetObjectId()))
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

	link, err := endpoint.ObjectHandler.CreateMultipartUploadRequest(object, int32(request.UploadPart))
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	response := &services.GetMultipartUploadLinkResponse{
		UploadLink: link,
	}

	return response, nil
}

func (endpoint *LoadEndpoints) CompleteMultipartUpload(ctx context.Context, request *services.CompleteMultipartUploadRequest) (*services.CompleteMultipartUploadResponse, error) {
	object, err := endpoint.ReadHandler.GetObject(uint(request.GetObjectId()))
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

	var completedParts []types.CompletedPart
	for _, part := range request.GetParts() {
		completedParts = append(completedParts, types.CompletedPart{
			ETag:       &part.Etag,
			PartNumber: int32(part.Part),
		})
	}

	err = endpoint.ObjectHandler.CompleteMultipartUpload(object, completedParts)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	response := &services.CompleteMultipartUploadResponse{}

	return response, nil
}
