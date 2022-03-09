package server

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"

	"github.com/ScienceObjectsDB/CORE-Server/models"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	v1storagemodels "github.com/ScienceObjectsDB/go-api/sciobjsdb/api/storage/models/v1"
	v1storageservices "github.com/ScienceObjectsDB/go-api/sciobjsdb/api/storage/services/v1"
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

func (endpoint *LoadEndpoints) CreateUploadLink(ctx context.Context, request *v1storageservices.CreateUploadLinkRequest) (*v1storageservices.CreateUploadLinkResponse, error) {
	requestID, err := uuid.Parse(request.GetId())
	if err != nil {
		log.Debug(err.Error())
		return nil, status.Error(codes.InvalidArgument, "could not parse dataset id")
	}

	object, err := endpoint.ReadHandler.GetObject(requestID)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	metadata, _ := metadata.FromIncomingContext(ctx)

	err = endpoint.AuthzHandler.Authorize(
		object.ProjectID,
		v1storagemodels.Right_RIGHT_READ,
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

	response := v1storageservices.CreateUploadLinkResponse{
		UploadLink: uploadLink,
	}

	return &response, nil
}

func (endpoint *LoadEndpoints) CreateDownloadLink(ctx context.Context, request *v1storageservices.CreateDownloadLinkRequest) (*v1storageservices.CreateDownloadLinkResponse, error) {
	requestID, err := uuid.Parse(request.GetId())
	if err != nil {
		log.Debug(err.Error())
		return nil, status.Error(codes.InvalidArgument, "could not parse dataset id")
	}

	object, err := endpoint.ReadHandler.GetObject(requestID)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	metadata, _ := metadata.FromIncomingContext(ctx)

	err = endpoint.AuthzHandler.Authorize(
		object.ProjectID,
		v1storagemodels.Right_RIGHT_READ,
		metadata)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	downloadLink, err := endpoint.ObjectHandler.CreateDownloadLink(object, request)
	if err != nil {
		log.Errorln(err.Error())
		return nil, err
	}

	objectStats, err := endpoint.StatsHandler.GetObjectStats(object.ID)
	if err != nil {
		log.Errorln(err.Error())
		return nil, err
	}

	protoObject, err := object.ToProtoModel(objectStats)
	if err != nil {
		log.Errorln(err.Error())
		return nil, status.Error(codes.Internal, "could not transform object into protobuf representation")
	}

	response := v1storageservices.CreateDownloadLinkResponse{
		DownloadLink: downloadLink,
		Object:       protoObject,
	}

	return &response, nil
}

func (endpoint *LoadEndpoints) CreateDownloadLinkBatch(ctx context.Context, request *v1storageservices.CreateDownloadLinkBatchRequest) (*v1storageservices.CreateDownloadLinkBatchResponse, error) {
	metadata, _ := metadata.FromIncomingContext(ctx)
	dlLinks := make([]*v1storageservices.CreateDownloadLinkResponse, len(request.GetRequests()))
	projectIDs := make(map[uuid.UUID]interface{})
	objectIDs := make([]uuid.UUID, len(request.GetRequests()))
	for i, request := range request.GetRequests() {
		requestID, err := uuid.Parse(request.GetId())
		if err != nil {
			log.Debug(err.Error())
			return nil, status.Error(codes.InvalidArgument, "could not parse dataset id")
		}

		objectIDs[i] = requestID
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
		err := endpoint.AuthzHandler.Authorize(projectID, v1storagemodels.Right_RIGHT_READ, metadata)
		if err != nil {
			log.Println(err.Error())
			return nil, err
		}
	}

	for i, object := range objects {
		link, err := endpoint.ObjectHandler.CreateDownloadLink(object, request.GetRequests()[i])
		if err != nil {
			log.Println(err.Error())
			return nil, err
		}

		objectStats, err := endpoint.StatsHandler.GetObjectStats(object.ID)
		if err != nil {
			log.Errorln(err.Error())
			return nil, err
		}

		protoObject, err := object.ToProtoModel(objectStats)
		if err != nil {
			log.Errorln(err.Error())
			return nil, status.Error(codes.Internal, "could not transform object into protobuf representation")
		}

		dlLinks[i] = &v1storageservices.CreateDownloadLinkResponse{
			DownloadLink: link,
			Object:       protoObject,
		}
	}

	response := &v1storageservices.CreateDownloadLinkBatchResponse{
		Links: dlLinks,
	}

	return response, nil
}

func (endpoint *LoadEndpoints) CreateDownloadLinkStream(request *v1storageservices.CreateDownloadLinkStreamRequest, responseStream v1storageservices.ObjectLoadService_CreateDownloadLinkStreamServer) error {
	var projectID uuid.UUID

	switch value := request.Query.(type) {
	case *v1storageservices.CreateDownloadLinkStreamRequest_Dataset:
		{
			datasetId, err := uuid.Parse(value.Dataset.GetDatasetId())
			if err != nil {
				log.Debug(err.Error())
				return status.Error(codes.InvalidArgument, "could not parse dataset id")
			}
			dataset, err := endpoint.ReadHandler.GetDataset(datasetId)
			if err != nil {
				log.Println(err.Error())
				return err
			}

			projectID = dataset.ProjectID
		}
	case *v1storageservices.CreateDownloadLinkStreamRequest_DatasetVersion:
		{
			datasetVersionID, err := uuid.Parse(value.DatasetVersion.GetDatasetVersionId())
			if err != nil {
				log.Debug(err.Error())
				return status.Error(codes.InvalidArgument, "could not parse dataset id")
			}
			dataset, err := endpoint.ReadHandler.GetDatasetVersion(datasetVersionID)
			if err != nil {
				log.Println(err.Error())
				return err
			}

			projectID = dataset.ProjectID
		}
	case *v1storageservices.CreateDownloadLinkStreamRequest_DateRange:
		{
			datasetID, err := uuid.Parse(value.DateRange.GetDatasetId())
			if err != nil {
				log.Debug(err.Error())
				return status.Error(codes.InvalidArgument, "could not parse dataset id")
			}

			dataset, err := endpoint.ReadHandler.GetDataset(datasetID)
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
		v1storagemodels.Right_RIGHT_READ,
		metadata)
	if err != nil {
		log.Println(err.Error())
		return err
	}

	readerErrGrp := errgroup.Group{}
	objectGroupsChan := make(chan []*models.ObjectGroup, 5)

	switch value := request.Query.(type) {
	case *v1storageservices.CreateDownloadLinkStreamRequest_Dataset:
		{
			readerErrGrp.Go(func() error {
				defer close(objectGroupsChan)

				datasetID, err := uuid.Parse(request.GetDataset().GetDatasetId())
				if err != nil {
					log.Debug(err.Error())
					return status.Error(codes.InvalidArgument, "could not parse dataset id")
				}

				return endpoint.ReadHandler.GetDatasetObjectGroupsBatches(datasetID, objectGroupsChan)
			})

		}
	case *v1storageservices.CreateDownloadLinkStreamRequest_DateRange:
		{
			readerErrGrp.Go(func() error {
				defer close(objectGroupsChan)

				datasetID, err := uuid.Parse(request.GetDataset().GetDatasetId())
				if err != nil {
					log.Debug(err.Error())
					return status.Error(codes.InvalidArgument, "could not parse dataset id")
				}
				return endpoint.ReadHandler.GetObjectGroupsInDateRangeBatches(datasetID, value.DateRange.Start.AsTime(), value.DateRange.End.AsTime(), objectGroupsChan)
			})
		}
	default:
		return status.Error(codes.Unimplemented, "unimplemented")
	}

	for objectGroupBatch := range objectGroupsChan {
		objectGroups := make([]*v1storagemodels.ObjectGroup, len(objectGroupBatch))
		links := make([]*v1storageservices.InnerLinksResponse, len(objectGroupBatch))
		for i, objectGroup := range objectGroupBatch {
			objectGroupStats, objectStatsList, err := endpoint.StatsHandler.GetObjectGroupStats(objectGroup)
			if err != nil {
				log.Errorln(err.Error())
				return err
			}

			protoObjectGroup, err := objectGroup.ToProtoModel(objectGroupStats, objectStatsList)
			if err != nil {
				log.Errorln(err.Error())
				return status.Error(codes.Internal, "could not transform objectgroup into protobuf representation")
			}
			objectGroups = append(objectGroups, protoObjectGroup)
			objectLinks := make([]string, len(objectGroup.Objects))
			for j, object := range objectGroup.Objects {
				link, err := endpoint.ObjectHandler.CreateDownloadLink(&object, &v1storageservices.CreateDownloadLinkRequest{})
				if err != nil {
					log.Println(err.Error())
					return err
				}
				objectLinks[j] = link
			}
			links[i] = &v1storageservices.InnerLinksResponse{
				ObjectLinks: objectLinks,
			}
		}

		batchResponse := &v1storageservices.CreateDownloadLinkStreamResponse{
			Links: &v1storageservices.LinksResponse{
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

func (endpoint *LoadEndpoints) StartMultipartUpload(ctx context.Context, request *v1storageservices.StartMultipartUploadRequest) (*v1storageservices.StartMultipartUploadResponse, error) {
	requestID, err := uuid.Parse(request.GetId())
	if err != nil {
		log.Debug(err.Error())
		return nil, status.Error(codes.InvalidArgument, "could not parse dataset id")
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

	objectStats, err := endpoint.StatsHandler.GetObjectStats(object.ID)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	protoObject, err := object.ToProtoModel(objectStats)
	if err != nil {
		log.Errorln(err.Error())
		return nil, status.Error(codes.Internal, "could not transform object into protobuf representation")
	}

	response := &v1storageservices.StartMultipartUploadResponse{
		Object: protoObject,
	}

	return response, nil
}

func (endpoint *LoadEndpoints) GetMultipartUploadLink(ctx context.Context, request *v1storageservices.GetMultipartUploadLinkRequest) (*v1storageservices.GetMultipartUploadLinkResponse, error) {
	objectID, err := uuid.Parse(request.GetObjectId())
	if err != nil {
		log.Debug(err.Error())
		return nil, status.Error(codes.InvalidArgument, "could not parse dataset id")
	}

	object, err := endpoint.ReadHandler.GetObject(objectID)
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

	link, err := endpoint.ObjectHandler.CreateMultipartUploadRequest(object, int32(request.UploadPart))
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	response := &v1storageservices.GetMultipartUploadLinkResponse{
		UploadLink: link,
	}

	return response, nil
}

func (endpoint *LoadEndpoints) CompleteMultipartUpload(ctx context.Context, request *v1storageservices.CompleteMultipartUploadRequest) (*v1storageservices.CompleteMultipartUploadResponse, error) {
	objectID, err := uuid.Parse(request.GetObjectId())
	if err != nil {
		log.Debug(err.Error())
		return nil, status.Error(codes.InvalidArgument, "could not parse dataset id")
	}

	object, err := endpoint.ReadHandler.GetObject(objectID)
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

	response := &v1storageservices.CompleteMultipartUploadResponse{}

	return response, nil
}
