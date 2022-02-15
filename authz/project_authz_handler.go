package authz

import (
	"errors"
	"fmt"

	v1storagemodels "github.com/ScienceObjectsDB/go-api/sciobjsdb/api/storage/models/v1"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"

	"google.golang.org/grpc/metadata"
	"gorm.io/gorm"
)

const API_TOKEN_ENTRY_KEY = "API_TOKEN"
const USER_TOKEN_ENTRY_KEY = "accesstoken"

type ProjectHandler struct {
	OAuth2Handler   *OAuth2Authz
	APITokenHandler *APITokenHandler
	DB              *gorm.DB
	JwtHandler      *JWTHandler
}

func (projectHandler *ProjectHandler) GetUserID(metadata metadata.MD) (uuid.UUID, error) {
	var userID uuid.UUID
	var err error
	if len(metadata.Get(API_TOKEN_ENTRY_KEY)) > 0 {
		userID, err = projectHandler.APITokenHandler.GetUserID(metadata.Get(API_TOKEN_ENTRY_KEY)[0])
		if err != nil {
			log.Println(err.Error())
			return uuid.UUID{}, err
		}
	}

	if len(metadata.Get(USER_TOKEN_ENTRY_KEY)) > 0 {
		userID, err = projectHandler.OAuth2Handler.GetUserID(metadata.Get(USER_TOKEN_ENTRY_KEY)[0])
		if err != nil {
			log.Println(err.Error())
			return uuid.UUID{}, err
		}
	}

	return userID, nil
}

func (projectHandler *ProjectHandler) Authorize(projectID uuid.UUID, requestedRight v1storagemodels.Right, metadata metadata.MD) error {
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

func (projectHandler *ProjectHandler) AuthorizeCreateProject(metadata metadata.MD) error {
	if len(metadata.Get(USER_TOKEN_ENTRY_KEY)) != 1 {
		return fmt.Errorf("could not authorize requested action")
	}

	token := metadata.Get(USER_TOKEN_ENTRY_KEY)[0]

	parsedToken, err := projectHandler.JwtHandler.VerifyAndParseToken(token)
	if err != nil {
		log.Println(err.Error())
		return errors.New("could not verify token")
	}

	var ok bool
	var claims *CustomClaim

	if claims, ok = parsedToken.Claims.(*CustomClaim); !ok || !parsedToken.Valid {
		return errors.New("could not verify token")
	}

	hasGroup := false
	for _, group := range claims.UserGroups {
		if group == "/sciobjsdb-test" {
			hasGroup = true
			break
		}
	}

	if !hasGroup {
		return fmt.Errorf("user not part of group sciobjsdb-test")
	}

	return nil
}

func (projectHandler *ProjectHandler) AuthorizeRead(metadata metadata.MD) error {
	if len(metadata.Get(USER_TOKEN_ENTRY_KEY)) != 1 {
		return fmt.Errorf("could not authorize requested action")
	}

	token := metadata.Get(USER_TOKEN_ENTRY_KEY)[0]

	parsedToken, err := projectHandler.JwtHandler.VerifyAndParseToken(token)
	if err != nil {
		log.Println(err.Error())
		return errors.New("could not verify token")
	}

	var ok bool
	var claims *CustomClaim

	if claims, ok = parsedToken.Claims.(*CustomClaim); !ok || !parsedToken.Valid {
		return errors.New("could not verify token")
	}

	hasGroup := false
	for _, group := range claims.UserGroups {
		if group == "/sciobjsdb-test" {
			hasGroup = true
			break
		}
	}

	if !hasGroup {
		return fmt.Errorf("user not part of group sciobjsdb-test")
	}

	return nil
}
