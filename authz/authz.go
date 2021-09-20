package authz

import (
	"fmt"
	"log"

	protoModels "github.com/ScienceObjectsDB/go-api/api/models/v1"
	"github.com/spf13/viper"
	"google.golang.org/grpc/metadata"
	"gorm.io/gorm"
)

type AuthInterface interface {
	GetUserID(metadata metadata.MD) (string, error)
	Authorize(projectID uint, requestedRight protoModels.Right, metadata metadata.MD) error
	AuthorizeCreateProject(metadata metadata.MD) error
}

func InitAuthHandlerFromConf(db *gorm.DB) (AuthInterface, error) {
	if viper.IsSet("OAuth2") {
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

		authzHandler := &ProjectHandler{
			OAuth2Handler:   oauth2Handler,
			APITokenHandler: apiTokenHandler,
			DB:              db,
			JwtHandler:      jwtHandler,
		}
		return authzHandler, nil
	}

	if viper.IsSet("Test") {
		return &TestHandler{}, nil
	}

	noValidAuthFound := fmt.Errorf("could not find any ")

	return nil, noValidAuthFound
}
