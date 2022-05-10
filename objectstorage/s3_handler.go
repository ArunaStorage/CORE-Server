package objectstorage

import (
	"context"
	"errors"
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

	app_config "github.com/ScienceObjectsDB/CORE-Server/config"
	v1storagemodels "github.com/ScienceObjectsDB/go-api/sciobjsdb/api/storage/models/v1"
	v1storageservices "github.com/ScienceObjectsDB/go-api/sciobjsdb/api/storage/services/v1"
)

// Default chunk size for chunked downloads
const S3ChunkSize = 1024 * 1024 * 5

// Maximum number of retries when creating a new bucket
const MAXCREATEBUCKETRETRY = 10

// S3ObjectStorageHandler Handles the interaction with the s3 based object storage data backends
type S3ObjectStorageHandler struct {
	S3Client          *s3.Client
	S3DownloadManager *manager.Downloader
	PresignClient     *s3.PresignClient
	S3Endpoint        string
	S3BucketPrefix    string
}

// Represents a downloaded byte chunk and its source object
type DownloadedBytesInfo struct {
	Object *models.Object
	Data   []byte
}

// Creates a new S3ObjectStorageHandler
func (s3Handler *S3ObjectStorageHandler) New(S3BucketPrefix string) (*S3ObjectStorageHandler, error) {
	s3Endpoint := viper.GetString(app_config.S3_ENDPOINT)
	s3Implementation := viper.GetString(app_config.S3_IMPLEMENTATION)

	cfg, err := config.LoadDefaultConfig(
		context.Background(),
		config.WithRegion("RegionOne"),
		config.WithEndpointResolver(aws.EndpointResolverFunc(
			func(service, region string) (aws.Endpoint, error) {
				return aws.Endpoint{
					URL: s3Endpoint,
				}, nil
			})),
	)

	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = false })

	switch s3Implementation {
	case "MINIO":
		client = s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true })
	}

	presignClient := s3.NewPresignClient(client)

	downloader := manager.NewDownloader(client)

	s3Handler.S3Endpoint = s3Endpoint
	s3Handler.S3Client = client
	s3Handler.PresignClient = presignClient
	s3Handler.S3BucketPrefix = S3BucketPrefix
	s3Handler.S3DownloadManager = downloader

	return s3Handler, nil
}

func (s3Handler *S3ObjectStorageHandler) CreateBucket(projectID uuid.UUID) (string, error) {
	i := 0

	var bucketname string
	for {
		bucketname = fmt.Sprintf("%v-%v-%v", s3Handler.S3BucketPrefix, i, projectID.String())
		_, err := s3Handler.S3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
			Bucket: &bucketname,
		})

		if err == nil {
			break
		}

		bucketname = "foo"

		log.Println(projectID)

		var bne *types.BucketAlreadyExists
		if errors.As(err, &bne) {
			log.Infof("bucket with name %v already exists", bucketname)
			i++
		}

		if errors.As(err, &bne) && i >= MAXCREATEBUCKETRETRY {
			err := fmt.Errorf("bucket with name %v already exists", bucketname)
			log.Errorf(err.Error())
			return "", err
		}

		if err != nil && !errors.As(err, &bne) {
			log.Error(err.Error())
			return "", err
		}
	}

	if viper.GetString(app_config.S3_IMPLEMENTATION) != "MINIO" {
		_, err := s3Handler.S3Client.PutBucketCors(context.Background(), &s3.PutBucketCorsInput{
			Bucket: aws.String(bucketname),
			CORSConfiguration: &types.CORSConfiguration{
				CORSRules: []types.CORSRule{
					{
						AllowedMethods: []string{"GET", "PUT"},
						AllowedOrigins: []string{"*"},
						AllowedHeaders: []string{"*"},
						ExposeHeaders:  []string{"*"},
					},
				},
			},
		})

		if err != nil {
			log.Println(err.Error())
			return "", err
		}

	}

	return bucketname, nil
}

// CreateLocation Creates a location in objectstorage that stores the object
func (s3Handler *S3ObjectStorageHandler) CreateLocation(projectID uuid.UUID, datasetID uuid.UUID, objectUUID uuid.UUID, filename string, bucketname string) models.Location {
	objectKey := fmt.Sprintf("%v/%v/%v/%v", projectID, datasetID, objectUUID, filename)
	location := models.Location{
		Endpoint:  s3Handler.S3Endpoint,
		Bucket:    bucketname,
		Key:       objectKey,
		ProjectID: projectID,
		DatasetID: datasetID,
		ObjectID:  objectUUID,
		Status:    v1storagemodels.Status_STATUS_INITIATING.String(),
	}

	return location
}

// CreateDownloadLink Generates a presigned download link for an object
func (s3Handler *S3ObjectStorageHandler) CreateDownloadLink(location *models.Location, request *v1storageservices.CreateDownloadLinkRequest) (string, error) {
	ctx := context.Background()

	objectInputConf := &s3.GetObjectInput{
		Bucket: &location.Bucket,
		Key:    &location.Key,
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
func (s3Handler *S3ObjectStorageHandler) CreateUploadLink(location *models.Location) (string, error) {
	ctx := context.Background()
	presignReq, err := s3Handler.PresignClient.PresignPutObject(ctx, &s3.PutObjectInput{
		Bucket: &location.Bucket,
		Key:    &location.Key,
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
func (s3Handler *S3ObjectStorageHandler) InitMultipartUpload(location *models.Location) (string, error) {
	ctx := context.Background()
	out, err := s3Handler.S3Client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: &location.Bucket,
		Key:    &location.Key,
	})
	if err != nil {
		log.Println(err.Error())
		return "", err
	}

	return *out.UploadId, nil
}

// CreateMultipartUploadRequest Generates a multipart upload link
func (s3Handler *S3ObjectStorageHandler) CreateMultipartUploadRequest(location *models.Location, partnumber int32) (string, error) {
	resp, err := s3Handler.PresignClient.PresignUploadPart(context.Background(), &s3.UploadPartInput{
		Bucket:     &location.Bucket,
		Key:        &location.Key,
		PartNumber: partnumber,
		UploadId:   &location.UploadID,
	})
	if err != nil {
		log.Println(err.Error())
		return "", err
	}

	return resp.URL, nil
}

// CompleteMultipartUpload Completes a multipart upload and tells the object storage to assemble the final object from the uploaded parts-
func (s3Handler *S3ObjectStorageHandler) CompleteMultipartUpload(location *models.Location, completedParts []types.CompletedPart) error {
	_, err := s3Handler.S3Client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{
		Bucket:   &location.Bucket,
		Key:      &location.Key,
		UploadId: &location.UploadID,
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

func (s3Handler *S3ObjectStorageHandler) DeleteObjects(locations []*models.Location) error {
	if len(locations) == 0 {
		return nil
	}

	bucket := locations[0].Bucket
	var deleteObjects []types.ObjectIdentifier
	for _, location := range locations {
		if bucket != location.Bucket && bucket != "" {
			err := fmt.Errorf("objects in batch deletes need to have the same bucket")
			log.Errorln(err.Error())
			return err
		}
		bucket = location.Bucket
		deleteObjects = append(deleteObjects, types.ObjectIdentifier{
			Key: &location.Key,
		})
	}

	_, err := s3Handler.S3Client.DeleteObjects(context.Background(), &s3.DeleteObjectsInput{
		Bucket: aws.String(bucket),
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

func (objectLoader *S3ObjectStorageHandler) ChunkedObjectDowload(location *models.Location, data chan []byte) error {
	headObject, err := objectLoader.S3Client.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: &location.Bucket,
		Key:    &location.Key,
	})
	if err != nil {
		log.Println(err.Error())
		return nil
	}

	sumReadBytes := 0
	toBeFinished := false
	for {
		if headObject.ContentLength == 0 {
			continue
		}

		readEndPos := sumReadBytes + S3ChunkSize
		if readEndPos > int(headObject.ContentLength) {
			readEndPos = (int(headObject.ContentLength) - 1) - sumReadBytes
			toBeFinished = true
		}

		rangeToRead := fmt.Sprintf("Range: bytes=%v-%v", sumReadBytes, readEndPos)

		buffer := make([]byte, readEndPos+sumReadBytes)
		writerBuffer := manager.NewWriteAtBuffer(buffer)
		readBytes, err := objectLoader.S3DownloadManager.Download(context.Background(), writerBuffer, &s3.GetObjectInput{
			Bucket: &location.Bucket,
			Key:    &location.Key,
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
