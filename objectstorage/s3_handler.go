package objectstorage

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"

	"github.com/ScienceObjectsDB/CORE-Server/models"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	services "github.com/ScienceObjectsDB/go-api/api/services/v1"
)

// Default chunk size for chunked downloads
const S3ChunkSize = 1024 * 1024 * 5

// S3ObjectStorageHandler Handles the interaction with the s3 based object storage data backends
type S3ObjectStorageHandler struct {
	S3Client          *s3.Client
	S3DownloadManager *manager.Downloader
	PresignClient     *s3.PresignClient
	S3Endpoint        string
	S3Bucket          string
}

// Represents a downloaded byte chunk and its source object
type DownloadedBytesInfo struct {
	Object *models.Object
	Data   []byte
}

// Creates a new S3ObjectStorageHandler
func (s3Handler *S3ObjectStorageHandler) New(s3Bucket string) (*S3ObjectStorageHandler, error) {
	endpoint := "https://s3.computational.bio.uni-giessen.de"
	if configEndpoint := viper.GetString("S3.Endpoint"); configEndpoint != "" {
		endpoint = configEndpoint
	}

	cfg, err := config.LoadDefaultConfig(
		context.Background(),
		config.WithRegion("RegionOne"),
		config.WithEndpointResolver(aws.EndpointResolverFunc(
			func(service, region string) (aws.Endpoint, error) {
				return aws.Endpoint{
					URL: endpoint,
				}, nil
			})),
	)

	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = false })

	//Testing endpoint, minio cannot use bucket path style with presigned urls
	if endpoint == "http://minio:9000" {
		client = s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true })
	}
	presignClient := s3.NewPresignClient(client)

	downloader := manager.NewDownloader(client)

	s3Handler.S3Endpoint = endpoint
	s3Handler.S3Client = client
	s3Handler.PresignClient = presignClient
	s3Handler.S3Bucket = s3Bucket
	s3Handler.S3DownloadManager = downloader

	return s3Handler, nil
}

// CreateLocation Creates a location in objectstorage that stores the object
func (s3Handler *S3ObjectStorageHandler) CreateLocation(projectID uuid.UUID, datasetID uuid.UUID, objectUUID uuid.UUID, filename string) models.Location {
	objectKey := fmt.Sprintf("%v/%v/%v/%v", projectID, datasetID, objectUUID, filename)
	location := models.Location{
		Endpoint: s3Handler.S3Endpoint,
		Bucket:   s3Handler.S3Bucket,
		Key:      objectKey,
	}

	return location
}

// CreateDownloadLink Generates a presigned download link for an object
func (s3Handler *S3ObjectStorageHandler) CreateDownloadLink(object *models.Object, request *services.CreateDownloadLinkRequest) (string, error) {
	ctx := context.Background()

	objectInputConf := &s3.GetObjectInput{
		Bucket: &object.Location.Bucket,
		Key:    &object.Location.Key,
	}

	if request.Range != nil {
		rangeReq := fmt.Sprintf("bytes=%d-%d", request.Range.StartByte, request.Range.EndByte)
		objectInputConf.Range = &rangeReq
	}

	presignReq, err := s3Handler.PresignClient.PresignGetObject(ctx, objectInputConf)
	if err != nil {
		log.Println(err.Error())
		return "", err
	}

	return presignReq.URL, nil
}

// CreateUploadLink Generates a presigned upload link for an object
func (s3Handler *S3ObjectStorageHandler) CreateUploadLink(object *models.Object) (string, error) {
	ctx := context.Background()
	presignReq, err := s3Handler.PresignClient.PresignPutObject(ctx, &s3.PutObjectInput{
		Bucket: &object.Location.Bucket,
		Key:    &object.Location.Key,
	})
	if err != nil {
		log.Println(err.Error())
		return "", err
	}

	return presignReq.URL, nil
}

// InitMultipartUpload Initiates a multipart upload for an object
// For details regarding multipart uploads please refer to the offical S3 documentation
// In short multipart uploads are intended to upload larger files
func (s3Handler *S3ObjectStorageHandler) InitMultipartUpload(object *models.Object) (string, error) {
	ctx := context.Background()
	out, err := s3Handler.S3Client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: &object.Location.Bucket,
		Key:    &object.Location.Key,
	})
	if err != nil {
		log.Println(err.Error())
		return "", err
	}

	return *out.UploadId, nil
}

// CreateMultipartUploadRequest Generates a multipart upload link
func (s3Handler *S3ObjectStorageHandler) CreateMultipartUploadRequest(object *models.Object, partnumber int32) (string, error) {
	resp, err := s3Handler.PresignClient.PresignUploadPart(context.Background(), &s3.UploadPartInput{
		Bucket:     &object.Location.Bucket,
		Key:        &object.Location.Key,
		PartNumber: partnumber,
		UploadId:   &object.UploadID,
	})
	if err != nil {
		log.Println(err.Error())
		return "", err
	}

	return resp.URL, nil
}

// CompleteMultipartUpload Completes a multipart upload and tells the object storage to assemble the final object from the uploaded parts-
func (s3Handler *S3ObjectStorageHandler) CompleteMultipartUpload(object *models.Object, completedParts []types.CompletedPart) error {
	_, err := s3Handler.S3Client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{
		Bucket:   &object.Location.Bucket,
		Key:      &object.Location.Key,
		UploadId: &object.UploadID,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: completedParts,
		},
	})

	if err != nil {
		log.Println(err.Error())
		return err
	}
	return nil
}

func (s3Handler *S3ObjectStorageHandler) DeleteObjects(objects []*models.Object) error {
	if len(objects) == 0 {
		return nil
	}

	var deleteObjects []types.ObjectIdentifier
	for _, object := range objects {
		deleteObjects = append(deleteObjects, types.ObjectIdentifier{
			Key: &object.Location.Key,
		})
	}

	_, err := s3Handler.S3Client.DeleteObjects(context.Background(), &s3.DeleteObjectsInput{
		Bucket: &s3Handler.S3Bucket,
		Delete: &types.Delete{
			Objects: deleteObjects,
		},
	})
	if err != nil {
		log.Println(err.Error())
		return err
	}

	return nil
}

func (objectLoader *S3ObjectStorageHandler) ChunkedObjectDowload(object *models.Object, data chan []byte) error {
	headObject, err := objectLoader.S3Client.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: &object.Location.Bucket,
		Key:    &object.Location.Key,
	})
	if err != nil {
		log.Println(err.Error())
		return nil
	}

	sumReadBytes := 0
	toBeFinished := false
	for {
		readEndPos := sumReadBytes + S3ChunkSize
		if readEndPos > int(headObject.ContentLength) {
			readEndPos = (int(headObject.ContentLength) - 1) - sumReadBytes
			toBeFinished = true
		}

		rangeToRead := fmt.Sprintf("Range: bytes=%v-%v", sumReadBytes, readEndPos)

		buffer := make([]byte, readEndPos+sumReadBytes)
		writerBuffer := manager.NewWriteAtBuffer(buffer)
		readBytes, err := objectLoader.S3DownloadManager.Download(context.Background(), writerBuffer, &s3.GetObjectInput{
			Bucket: &object.Location.Bucket,
			Key:    &object.Location.Key,
			Range:  aws.String(rangeToRead),
		})
		if err != nil {
			log.Println(err.Error())
			return nil
		}

		data <- writerBuffer.Bytes()

		sumReadBytes = sumReadBytes + int(readBytes)

		if toBeFinished {
			return nil
		}
	}
}
