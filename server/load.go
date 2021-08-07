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
		UploadLink: downloadLink,
	}

	return &response, nil
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
