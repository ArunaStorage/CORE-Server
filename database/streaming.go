package database

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"net/url"

	"github.com/ScienceObjectsDB/CORE-Server/models"
	services "github.com/ScienceObjectsDB/go-api/api/services/v1"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

type Streaming struct {
	*Common
	StreamingEndpoint string
	SigningSecret     string
}

func (handler *Streaming) CreateStreamingEntry(request *services.GetObjectGroupsStreamRequest, projectID uint) (string, error) {
	var url string
	var err error

	switch value := request.Query.(type) {
	case *services.GetObjectGroupsStreamRequest_GroupIds:
		url, err = handler.createObjectGroupsRequest(value.GroupIds.GetObjectGroups(), uint(request.GetDatasetId()), projectID)
	case *services.GetObjectGroupsStreamRequest_Dataset:
		url, err = handler.createResourceObjectGroupsURL(uint(request.GetDatasetId()), "/dataset")
	case *services.GetObjectGroupsStreamRequest_DatasetVersion:
		url, err = handler.createResourceObjectGroupsURL(uint(request.GetDatasetId()), "/datasetversion")
	}

	if err != nil {
		log.Println(err.Error())
		return "", err
	}

	return url, nil
}

func (handler *Streaming) createResourceObjectGroupsURL(datasetID uint, resourcePath string, queryParams ...string) (string, error) {
	saltBytes := make([]byte, 64)
	_, err := rand.Read(saltBytes)
	if err != nil {
		log.Println(err.Error())
		return "", err
	}

	if err != nil {
		log.Println(err.Error())
		return "", err
	}

	escapedSalt := url.QueryEscape(string(saltBytes))

	parseBaseURL, err := url.Parse(handler.StreamingEndpoint)
	if err != nil {
		log.Println(err.Error())
		return "", err
	}

	parseBaseURL.Path = resourcePath

	q := parseBaseURL.Query()

	q.Set("id", fmt.Sprintf("%v", datasetID))
	q.Set("salt", escapedSalt)

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

	parseBaseURL.RawQuery = q.Encode()

	hmac, err := hmac_sha256([]byte(handler.SigningSecret), saltBytes, []byte(parseBaseURL.String()))
	if err != nil {
		log.Println(err.Error())
		return "", err
	}

	q = parseBaseURL.Query()
	q.Set("sign", string(hmac))

	parseBaseURL.RawQuery = q.Encode()

	return parseBaseURL.String(), nil
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
