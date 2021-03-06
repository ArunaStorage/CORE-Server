package database

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"net/url"

	"github.com/ScienceObjectsDB/CORE-Server/models"
	"github.com/ScienceObjectsDB/CORE-Server/signing"
	v1storageservices "github.com/ScienceObjectsDB/go-api/sciobjsdb/api/storage/services/v1"
	"github.com/cockroachdb/cockroach-go/v2/crdb/crdbgorm"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

type Streaming struct {
	*Common
	StreamingEndpoint string
	SigningSecret     string
}

func (handler *Streaming) CreateStreamingLink(request *v1storageservices.GetObjectGroupsStreamLinkRequest, projectID uuid.UUID) (string, error) {
	var url string
	var err error

	switch value := request.Query.(type) {
	case *v1storageservices.GetObjectGroupsStreamLinkRequest_GroupIds:
		{
			var datasetID uuid.UUID
			datasetID, err = uuid.Parse(request.GetGroupIds().GetDatasetId())
			if err != nil {
				log.Debug(err.Error())
				return "", err
			}
			url, err = handler.createObjectGroupsRequest(value.GroupIds.GetObjectGroups(), datasetID, projectID)
		}
	case *v1storageservices.GetObjectGroupsStreamLinkRequest_Dataset:
		{
			var datasetID uuid.UUID
			datasetID, err = uuid.Parse(request.GetDataset().GetDatasetId())
			if err != nil {
				log.Debug(err.Error())
				return "", err
			}
			url, err = handler.createResourceObjectGroupsURL(datasetID, "/dataset")
		}
	case *v1storageservices.GetObjectGroupsStreamLinkRequest_DatasetVersion:
		{
			var datasetVersionID uuid.UUID
			datasetVersionID, err = uuid.Parse(request.GetDatasetVersion().GetDatasetVersionId())
			if err != nil {
				log.Debug(err.Error())
				return "", err
			}
			url, err = handler.createResourceObjectGroupsURL(datasetVersionID, "/datasetversion")
		}
	default:
		return "", fmt.Errorf("could not find request type")
	}

	if err != nil {
		log.Println(err.Error())
		return "", err
	}

	return url, nil
}

func (handler *Streaming) createResourceObjectGroupsURL(resourceID uuid.UUID, resourcePath string, queryParams ...string) (string, error) {
	saltBytes := make([]byte, 64)
	_, err := rand.Read(saltBytes)
	if err != nil {
		log.Println(err.Error())
		return "", err
	}

	parsedBaseURL, err := url.Parse(handler.StreamingEndpoint)
	if err != nil {
		log.Println(err.Error())
		return "", err
	}

	parsedBaseURL.Path = resourcePath

	q := parsedBaseURL.Query()

	q.Set("id", fmt.Sprintf("%v", resourceID))
	var key string
	var value string

	for i, queryParam := range queryParams {
		if i%2 == 0 {
			key = queryParam
		}
		if i%2 == 1 {
			value = url.QueryEscape(queryParam)
			q.Set(key, value)
		}
	}

	parsedBaseURL.RawQuery = q.Encode()

	signedURL, err := signing.SignURL([]byte(handler.SigningSecret), parsedBaseURL)
	if err != nil {
		log.Println(err.Error())
		return "", err
	}

	return signedURL.String(), nil
}

func (handler *Streaming) createObjectGroupsRequest(objectGroupIDs []string, datasetID uuid.UUID, projectID uuid.UUID) (string, error) {
	rndBytes := make([]byte, 64)
	_, err := rand.Read(rndBytes)
	if err != nil {
		log.Println(err.Error())
		return "", err
	}

	base64Secret := base64.StdEncoding.EncodeToString(rndBytes)

	objectRevisionGroups := make([]models.ObjectGroupRevision, len(objectGroupIDs))
	for i, objectGroupID := range objectGroupIDs {
		objectGroupIDParsed, err := uuid.Parse(objectGroupID)
		if err != nil {
			log.Debug(objectGroupIDParsed)
			return "", err
		}

		objectGroupRevisions := models.ObjectGroupRevision{}
		objectGroupRevisions.ID = objectGroupIDParsed
		objectRevisionGroups[i] = objectGroupRevisions
	}

	uuid := uuid.NewString()

	entry := models.StreamingEntry{
		UUID:         uuid,
		Secret:       base64Secret,
		DatasetID:    datasetID,
		ProjectID:    projectID,
		ObjectGroups: objectRevisionGroups,
	}

	err = crdbgorm.ExecuteTx(context.Background(), handler.DB, nil, func(tx *gorm.DB) error {
		return tx.Save(&entry).Error
	})

	if err != nil {
		log.Error(err.Error())
		return "", err
	}

	mac := hmac.New(sha256.New, []byte(base64Secret))
	_, err = mac.Write([]byte(uuid))
	if err != nil {
		log.Println(err.Error())
		return "", err
	}

	url := fmt.Sprintf("%v?uuid=%v&hmac=%v", handler.StreamingEndpoint, uuid, mac.Sum(nil))

	return url, nil
}
