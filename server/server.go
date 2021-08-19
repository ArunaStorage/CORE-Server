package server

import (
	"fmt"
	"net"

	"github.com/ScienceObjectsDB/CORE-Server/authz"
	"github.com/ScienceObjectsDB/CORE-Server/database"
	"github.com/ScienceObjectsDB/CORE-Server/objectstorage"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"google.golang.org/grpc"

	services "github.com/ScienceObjectsDB/go-api/api/services/v1"
)

type Endpoints struct {
	ReadHandler   *database.Read
	CreateHandler *database.Create
	UpdateHandler *database.Update
	DeleteHandler *database.Delete
	AuthzHandler  authz.AuthInterface
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

	commonHandler := database.Common{
		DB:        db,
		S3Handler: objectHandler,
	}

	jwtHandler, err := authz.NewJWTHandler()
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	oauth2Handler, err := authz.NewOAuth2Authz(db, jwtHandler)
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
		JwtHandler:      jwtHandler,
	}

	endpoints := &Endpoints{
		ReadHandler: &database.Read{
			Common: &commonHandler,
		},
		CreateHandler: &database.Create{Common: &commonHandler},
		ObjectHandler: objectHandler,
		UpdateHandler: &database.Update{
			Common: &commonHandler,
		},
		DeleteHandler: &database.Delete{
			Common: &commonHandler,
		},
		AuthzHandler: authzHandler,
	}

	return endpoints, nil
}
