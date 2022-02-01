package authz

import (
	"fmt"
	"log"

	"github.com/ScienceObjectsDB/CORE-Server/config"
	protoModels "github.com/ScienceObjectsDB/go-api/api/models/v1"
	"github.com/google/uuid"
	"github.com/spf13/viper"
	"google.golang.org/grpc/metadata"
	"gorm.io/gorm"
)

type AuthInterface interface {
	GetUserID(metadata metadata.MD) (uuid.UUID, error)
	Authorize(projectID uuid.UUID, requestedRight protoModels.Right, metadata metadata.MD) error
	AuthorizeCreateProject(metadata metadata.MD) error
	AuthorizeRead(metadata metadata.MD) error
}

func InitAuthHandlerFromConf(db *gorm.DB) (AuthInterface, error) {
	authType := viper.GetString(config.AUTHENTICATION_TYPE)

	var err error
	var authzHandler AuthInterface

	switch authType {
	case "INSECURE":
		authzHandler = &TestHandler{}
	case "OIDC":
		jwtHandler, err := NewJWTHandler()
		if err != nil {
			log.Println(err.Error())
			return nil, err
		}

		oauth2Handler, err := NewOAuth2Authz(db, jwtHandler)
		if err != nil {
			log.Println(err.Error())
			return nil, err
		}

		apiTokenHandler := &APITokenHandler{
			DB: db,
		}

		authzHandler = &ProjectHandler{
			OAuth2Handler:   oauth2Handler,
			APITokenHandler: apiTokenHandler,
			DB:              db,
			JwtHandler:      jwtHandler,
		}
		return authzHandler, nil
	default:
		err = fmt.Errorf("could not find any valid authentication method, requires: [INSECURE, OIDC]")
	}

	return authzHandler, err
}
