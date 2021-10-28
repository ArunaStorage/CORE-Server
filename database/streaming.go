package database

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"net/url"

	"github.com/ScienceObjectsDB/CORE-Server/models"
	"github.com/ScienceObjectsDB/CORE-Server/signing"
	services "github.com/ScienceObjectsDB/go-api/api/services/v1"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

type Streaming struct {
	*Common
	StreamingEndpoint string
	SigningSecret     string
}

func (handler *Streaming) CreateStreamingLink(request *services.GetObjectGroupsStreamLinkRequest, projectID uuid.UUID) (string, error) {
	var url string
	var err error

	switch value := request.Query.(type) {
	case *services.GetObjectGroupsStreamLinkRequest_GroupIds:
		{
			var datasetID uuid.UUID
			datasetID, err = uuid.Parse(request.GetDataset().GetDatasetId())
			if err != nil {
				log.Debug(err.Error())
				return "", err
			}
			url, err = handler.createObjectGroupsRequest(value.GroupIds.GetObjectGroups(), datasetID, projectID)
		}
	case *services.GetObjectGroupsStreamLinkRequest_Dataset:
		{
			var datasetID uuid.UUID
			datasetID, err = uuid.Parse(request.GetDataset().GetDatasetId())
			if err != nil {
				log.Debug(err.Error())
				return "", err
			}
			url, err = handler.createResourceObjectGroupsURL(datasetID, "/dataset")
		}
	case *services.GetObjectGroupsStreamLinkRequest_DatasetVersion:
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

	objectGroups := make([]models.ObjectGroup, len(objectGroupIDs))
	for i, objectGroupID := range objectGroupIDs {
		objectGroupIDParsed, err := uuid.Parse(objectGroupID)
		if err != nil {
			log.Debug(objectGroupIDParsed)
			return "", err
		}

		objectGroup := models.ObjectGroup{}
		objectGroup.ID = objectGroupIDParsed
		objectGroups[i] = objectGroup
	}

	uuid := uuid.NewString()

	entry := models.StreamingEntry{
		UUID:         uuid,
		Secret:       base64Secret,
		DatasetID:    datasetID,
		ProjectID:    projectID,
		ObjectGroups: objectGroups,
	}

	if err := handler.DB.Save(&entry).Error; err != nil {
		log.Println(err.Error())
		return "", fmt.Errorf("could not save streaming config")
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
