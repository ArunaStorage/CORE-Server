package server

import (
	"fmt"
	"net"

	log "github.com/sirupsen/logrus"

	"google.golang.org/grpc"

	"github.com/ScienceObjectsDB/CORE-Server/authz"
	"github.com/ScienceObjectsDB/CORE-Server/database"
	"github.com/ScienceObjectsDB/CORE-Server/handler"
	"github.com/ScienceObjectsDB/CORE-Server/objectstorage"
	services "github.com/ScienceObjectsDB/go-api/api/services/v1"
	"github.com/spf13/viper"
)

type Endpoints struct {
	ReadHandler   *handler.Read
	CreateHandler *handler.Create
	UpdateHandler *handler.Update
	DeleteHandler *handler.Delete
	AuthzHandler  *authz.ProjectHandler
	ObjectHandler *objectstorage.S3ObjectStorageHandler
}

type Server struct {
}

func Run(host string, port uint16) error {
	listener, err := net.Listen("tcp", fmt.Sprintf("%v:%v", host, port))
	if err != nil {
		log.Println(err.Error())
		return err
	}

	var opts []grpc.ServerOption

	grpcServer := grpc.NewServer(opts...)

	endpoints, err := createGenericEndpoint()
	if err != nil {
		log.Println(err.Error())
		return err
	}

	projectEndpoints, err := NewProjectEndpoints(endpoints)
	if err != nil {
		log.Println(err.Error())
		return err
	}

	datasetEndpoints, err := NewDatasetEndpoints(endpoints)
	if err != nil {
		log.Println(err.Error())
		return err
	}

	objectEndpoints, err := NewObjectEndpoints(endpoints)
	if err != nil {
		log.Println(err.Error())
		return err
	}

	loadEndpoints, err := NewLoadEndpoints(endpoints)
	if err != nil {
		log.Println(err.Error())
		return err
	}

	services.RegisterProjectServiceServer(grpcServer, projectEndpoints)
	services.RegisterDatasetServiceServer(grpcServer, datasetEndpoints)
	services.RegisterDatasetObjectsServiceServer(grpcServer, objectEndpoints)
	services.RegisterObjectLoadServiceServer(grpcServer, loadEndpoints)

	err = grpcServer.Serve(listener)
	if err != nil {
		log.Println(err.Error())
		return err
	}

	return nil
}

func createGenericEndpoint() (*Endpoints, error) {
	dbHost := viper.GetString("DB.Host")
	dbPort := viper.GetUint("DB.Port")
	dbName := viper.GetString("DB.Name")
	dbUsername := viper.GetString("DB.Username")

	db, err := database.NewPsqlDB(dbHost, uint64(dbPort), dbUsername, dbName)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	bucketName := viper.GetString("S3.Bucket")

	objectHandler := &objectstorage.S3ObjectStorageHandler{}
	objectHandler, err = objectHandler.New(bucketName)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	commonHandler := handler.Common{
		DB:        db,
		S3Handler: objectHandler,
	}

	oauth2Handler, err := authz.NewOAuth2Authz()
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	apiTokenHandler := &authz.APITokenHandler{
		DB: db,
	}

	authzHandler := &authz.ProjectHandler{
		OAuth2Handler:   oauth2Handler,
		APITokenHandler: apiTokenHandler,
		DB:              db,
	}

	endpoints := &Endpoints{
		ReadHandler: &handler.Read{
			Common: &commonHandler,
		},
		CreateHandler: &handler.Create{Common: &commonHandler},
		ObjectHandler: objectHandler,
		UpdateHandler: &handler.Update{
			Common: &commonHandler,
		},
		DeleteHandler: &handler.Delete{
			Common: &commonHandler,
		},
		AuthzHandler: authzHandler,
	}

	return endpoints, nil
}
