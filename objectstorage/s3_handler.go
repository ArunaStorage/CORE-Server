package objectstorage

import (
	"context"
	"fmt"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"

	"github.com/ScienceObjectsDB/CORE-Server/models"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type S3ObjectStorageHandler struct {
	S3Client      *s3.Client
	PresignClient *s3.PresignClient
	S3Endpoint    string
	S3Bucket      string
}

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

	client := s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true })
	presignClient := s3.NewPresignClient(client)

	s3Handler.S3Endpoint = endpoint
	s3Handler.S3Client = client
	s3Handler.PresignClient = presignClient
	s3Handler.S3Bucket = s3Bucket

	return s3Handler, nil
}

func (s3Handler *S3ObjectStorageHandler) CreateLocation(projectID uint, datasetID uint, revision uint64, filename string) *models.Location {
	revision_name := fmt.Sprintf("revision-%v", revision)

	objectKey := fmt.Sprintf("%v/%v/%v/%v", projectID, datasetID, revision_name, filename)
	location := models.Location{
		Endpoint: s3Handler.S3Endpoint,
		Bucket:   s3Handler.S3Bucket,
		Key:      objectKey,
	}

	return &location
}

func (s3Handler *S3ObjectStorageHandler) CreateDownloadLink(object *models.Object) (string, error) {
	ctx := context.Background()
	presignReq, err := s3Handler.PresignClient.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: &object.Location.Bucket,
		Key:    &object.Location.Key,
	})
	if err != nil {
		log.Println(err.Error())
		return "", err
	}

	return presignReq.URL, nil
}

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
