package server

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"

	"github.com/ScienceObjectsDB/CORE-Server/models"
	protoModels "github.com/ScienceObjectsDB/go-api/api/models/v1"
	services "github.com/ScienceObjectsDB/go-api/api/services/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
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
	object, err := endpoint.ReadHandler.GetObject(uuid.MustParse(request.GetId()))
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
	object, err := endpoint.ReadHandler.GetObject(uuid.MustParse(request.GetId()))
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
	projectIDs := make(map[uuid.UUID]interface{})
	objectIDs := make([]uuid.UUID, len(request.GetRequests()))
	for i, request := range request.GetRequests() {
		objectIDs[i] = uuid.MustParse(request.GetId())
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

func (endpoint *LoadEndpoints) CreateDownloadLinkStream(request *services.CreateDownloadLinkStreamRequest, responseStream services.ObjectLoadService_CreateDownloadLinkStreamServer) error {
	var projectID uuid.UUID

	switch value := request.Query.(type) {
	case *services.CreateDownloadLinkStreamRequest_Dataset:
		{
			dataset, err := endpoint.ReadHandler.GetDataset(uuid.MustParse(value.Dataset.GetDatasetId()))
			if err != nil {
				log.Println(err.Error())
				return err
			}

			projectID = dataset.ProjectID
		}
	case *services.CreateDownloadLinkStreamRequest_DatasetVersion:
		{
			dataset, err := endpoint.ReadHandler.GetDatasetVersion(uuid.MustParse(value.DatasetVersion.GetDatasetVersionId()))
			if err != nil {
				log.Println(err.Error())
				return err
			}

			projectID = dataset.ProjectID
		}
	case *services.CreateDownloadLinkStreamRequest_DateRange:
		{
			dataset, err := endpoint.ReadHandler.GetDataset(uuid.MustParse(value.DateRange.GetDatasetId()))
			if err != nil {
				log.Println(err.Error())
				return err
			}

			projectID = dataset.ProjectID
		}
	default:
		return status.Error(codes.Unauthenticated, "could not authorize requested action")
	}

	metadata, _ := metadata.FromIncomingContext(responseStream.Context())

	err := endpoint.AuthzHandler.Authorize(
		projectID,
		protoModels.Right_READ,
		metadata)
	if err != nil {
		log.Println(err.Error())
		return err
	}

	readerErrGrp := errgroup.Group{}
	objectGroupsChan := make(chan []*models.ObjectGroup, 5)

	switch value := request.Query.(type) {
	case *services.CreateDownloadLinkStreamRequest_Dataset:
		{
			readerErrGrp.Go(func() error {
				defer close(objectGroupsChan)
				return endpoint.ReadHandler.GetDatasetObjectGroupsBatches(uuid.MustParse(value.Dataset.GetDatasetId()), objectGroupsChan)
			})

		}
	case *services.CreateDownloadLinkStreamRequest_DateRange:
		{
			readerErrGrp.Go(func() error {
				defer close(objectGroupsChan)
				return endpoint.ReadHandler.GetObjectGroupsInDateRangeBatches(uuid.MustParse(value.DateRange.GetDatasetId()), value.DateRange.Start.AsTime(), value.DateRange.End.AsTime(), objectGroupsChan)
			})
		}
	default:
		return status.Error(codes.Unimplemented, "unimplemented")
	}

	for objectGroupBatch := range objectGroupsChan {
		objectGroups := make([]*protoModels.ObjectGroup, len(objectGroupBatch))
		links := make([]*services.InnerLinksResponse, len(objectGroupBatch))
		for i, objectGroup := range objectGroupBatch {
			objectGroups = append(objectGroups, objectGroup.ToProtoModel())
			objectLinks := make([]string, len(objectGroup.Objects))
			for j, object := range objectGroup.Objects {
				link, err := endpoint.ObjectHandler.CreateDownloadLink(&object)
				if err != nil {
					log.Println(err.Error())
					return err
				}
				objectLinks[j] = link
			}
			links[i] = &services.InnerLinksResponse{
				ObjectLinks: objectLinks,
			}
		}

		batchResponse := &services.CreateDownloadLinkStreamResponse{
			Links: &services.LinksResponse{
				ObjectGroups:     objectGroups,
				ObjectGroupLinks: links,
			},
		}

		err := responseStream.Send(batchResponse)
		if err != nil {
			log.Println(err.Error())
			return err
		}
	}

	return nil
}

func (endpoint *LoadEndpoints) StartMultipartUpload(ctx context.Context, request *services.StartMultipartUploadRequest) (*services.StartMultipartUploadResponse, error) {
	object, err := endpoint.ReadHandler.GetObject(uuid.MustParse(request.GetId()))
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
	object, err := endpoint.ReadHandler.GetObject(uuid.MustParse(request.GetObjectId()))
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
	object, err := endpoint.ReadHandler.GetObject(uuid.MustParse(request.GetObjectId()))
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
