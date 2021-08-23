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

func (handler *Streaming) CreateStreamingLink(request *services.GetObjectGroupsStreamLinkRequest, projectID uint) (string, error) {
	var url string
	var err error

	switch value := request.Query.(type) {
	case *services.GetObjectGroupsStreamLinkRequest_GroupIds:
		url, err = handler.createObjectGroupsRequest(value.GroupIds.GetObjectGroups(), uint(request.GetDataset().GetDatasetId()), projectID)
	case *services.GetObjectGroupsStreamLinkRequest_Dataset:
		url, err = handler.createResourceObjectGroupsURL(uint(request.GetDataset().GetDatasetId()), "/dataset")
	case *services.GetObjectGroupsStreamLinkRequest_DatasetVersion:
		url, err = handler.createResourceObjectGroupsURL(uint(request.GetDatasetVersion().GetDatasetVersion()), "/datasetversion")
	default:
		return "", fmt.Errorf("could not find request type")
	}

	if err != nil {
		log.Println(err.Error())
		return "", err
	}

	return url, nil
}

func (handler *Streaming) createResourceObjectGroupsURL(resourceID uint, resourcePath string, queryParams ...string) (string, error) {
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

func (handler *Streaming) createObjectGroupsRequest(objectGroupIDs []uint64, datasetID uint, projectID uint) (string, error) {
	rndBytes := make([]byte, 64)
	_, err := rand.Read(rndBytes)
	if err != nil {
		log.Println(err.Error())
		return "", err
	}

	base64Secret := base64.StdEncoding.EncodeToString(rndBytes)

	var objectGroups []models.ObjectGroup
	for _, objectGroupID := range objectGroupIDs {
		objectGroup := models.ObjectGroup{}
		objectGroup.ID = uint(objectGroupID)
		objectGroups = append(objectGroups, objectGroup)
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

func hmac_sha256(password, salt, msg []byte) ([]byte, error) {
	mac := hmac.New(sha256.New, password)
	_, err := mac.Write(salt)
	if err != nil {
		return nil, err
	}

	_, err = mac.Write(msg)
	if err != nil {
		return nil, err
	}

	return mac.Sum(nil), nil
}
