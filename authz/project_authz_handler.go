package authz

import (
	"fmt"

	log "github.com/sirupsen/logrus"

	protoModels "github.com/ScienceObjectsDB/go-api/api/models/v1"
	"google.golang.org/grpc/metadata"
	"gorm.io/gorm"
)

const API_TOKEN_ENTRY_KEY = "API_TOKEN"
const USER_TOKEN_ENTRY_KEY = "accesstoken"

type ProjectHandler struct {
	OAuth2Handler   *OAuth2Authz
	APITokenHandler *APITokenHandler
	DB              *gorm.DB
}

func (projectHandler *ProjectHandler) GetUserID(metadata metadata.MD) (string, error) {
	var userID string
	var err error
	if len(metadata.Get(API_TOKEN_ENTRY_KEY)) > 0 {
		userID, err = projectHandler.APITokenHandler.GetUserID(metadata.Get(API_TOKEN_ENTRY_KEY)[0])
		if err != nil {
			log.Println(err.Error())
			return "", err
		}
	}

	if len(metadata.Get(USER_TOKEN_ENTRY_KEY)) > 0 {
		userID, err = projectHandler.OAuth2Handler.GetUserID(metadata.Get(USER_TOKEN_ENTRY_KEY)[0])
		if err != nil {
			log.Println(err.Error())
			return "", err
		}
	}

	return userID, nil
}

func (projectHandler *ProjectHandler) Authorize(projectID uint, requestedRight protoModels.Right, metadata metadata.MD) error {
	if len(metadata.Get(API_TOKEN_ENTRY_KEY)) > 0 {
		_, err := projectHandler.APITokenHandler.Authorize(metadata.Get(API_TOKEN_ENTRY_KEY)[0], projectID)
		if err != nil {
			log.Println(err.Error())
			return fmt.Errorf("could not authorize requested action")
		}

		return nil
	}

	if len(metadata.Get(USER_TOKEN_ENTRY_KEY)) > 0 {
		_, err := projectHandler.OAuth2Handler.Authorize(metadata.Get(USER_TOKEN_ENTRY_KEY)[0], projectID)
		if err != nil {
			log.Println(err.Error())
			return fmt.Errorf("could not authorize requested action")
		}

		return nil
	}

	return fmt.Errorf("could not authorize requested action")
}
