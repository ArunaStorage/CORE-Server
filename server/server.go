package server

import (
	"fmt"
	"net"
	"os"

	"github.com/ScienceObjectsDB/CORE-Server/authz"
	"github.com/ScienceObjectsDB/CORE-Server/config"
	"github.com/ScienceObjectsDB/CORE-Server/database"
	"github.com/ScienceObjectsDB/CORE-Server/eventstreaming"
	"github.com/ScienceObjectsDB/CORE-Server/objectstorage"
	"github.com/ScienceObjectsDB/CORE-Server/streamingserver"
	"golang.org/x/sync/errgroup"
	"gorm.io/gorm"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"google.golang.org/grpc"

	v1notficationservices "github.com/ScienceObjectsDB/go-api/sciobjsdb/api/notification/services/v1"
	v1storageservices "github.com/ScienceObjectsDB/go-api/sciobjsdb/api/storage/services/v1"
)

// A generic structs for the gRPC endpoint that contains all relevant database handler interfaces
// This is meant to be reused in the individual gRPC service implementation
// The implementation of the individual services is done in separate structs.
// Usually endpoints functions of the services do only perform authorization. All further calls regarding data management with Objectstorage and DB
// are delegated to separate function.
type Endpoints struct {
	ReadHandler         *database.Read
	CreateHandler       *database.Create
	UpdateHandler       *database.Update
	DeleteHandler       *database.Delete
	AuthzHandler        authz.AuthInterface
	ObjectHandler       *objectstorage.S3ObjectStorageHandler
	ObjectStreamhandler *database.Streaming
	EventStreamMgmt     eventstreaming.EventStreamMgmt
}

type Server struct {
}

// Starts the gRPC and the data streaming server.
func Run() error {
	host := viper.GetString(config.SERVER_HOST)
	gRPCPort := viper.GetUint(config.SERVER_PORT)

	grpcListener, err := net.Listen("tcp", fmt.Sprintf("%v:%v", host, gRPCPort))
	if err != nil {
		log.Errorln(err.Error())
		return err
	}

	var opts []grpc.ServerOption

	grpcServer := grpc.NewServer(opts...)

	endpoints, err := createGenericEndpoint()
	if err != nil {
		log.Errorln(err.Error())
		return err
	}

	projectEndpoints, err := NewProjectEndpoints(endpoints)
	if err != nil {
		log.Errorln(err.Error())
		return err
	}

	datasetEndpoints, err := NewDatasetEndpoints(endpoints)
	if err != nil {
		log.Errorln(err.Error())
		return err
	}

	objectEndpoints, err := NewObjectEndpoints(endpoints)
	if err != nil {
		log.Errorln(err.Error())
		return err
	}

	loadEndpoints, err := NewLoadEndpoints(endpoints)
	if err != nil {
		log.Errorln(err.Error())
		return err
	}

	notificationEndpoints, err := NewNotificationEndpoints(endpoints)
	if err != nil {
		log.Errorln(err.Error())
		return err
	}

	streamSigningSecret := os.Getenv("STREAMINGSIGNSECRET")

	streamingServer := streamingserver.DataStreamingServer{
		SigningSecret: streamSigningSecret,
		ReadHandler:   datasetEndpoints.ReadHandler,
		ObjectHandler: datasetEndpoints.ObjectHandler,
	}

	serverErrGrp := errgroup.Group{}
	serverErrGrp.Go(func() error {
		return streamingServer.Run()
	})

	v1storageservices.RegisterProjectServiceServer(grpcServer, projectEndpoints)
	v1storageservices.RegisterDatasetServiceServer(grpcServer, datasetEndpoints)
	v1storageservices.RegisterDatasetObjectsServiceServer(grpcServer, objectEndpoints)
	v1storageservices.RegisterObjectLoadServiceServer(grpcServer, loadEndpoints)
	v1notficationservices.RegisterUpdateNotificationServiceServer(grpcServer, notificationEndpoints)

	serverErrGrp.Go(func() error {
		log.Println(fmt.Sprintf("Starting grpc service on interface %v and port %v", host, gRPCPort))
		return grpcServer.Serve(grpcListener)
	})

	return serverErrGrp.Wait()
}

// Creates the endpoint config based on the provided config.
func createGenericEndpoint() (*Endpoints, error) {
	streamingEndpoint := viper.GetString(config.STREAMING_ENDPOINT)
	streamSigningSecret := os.Getenv(config.STREAMING_SECRET_ENV_VAR)

	var db *gorm.DB
	var err error

	db, err = database.InitDatabaseConnection()
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	bucketName := viper.GetString(config.S3_BUCKET_PREFIX)

	objectHandler := &objectstorage.S3ObjectStorageHandler{}
	objectHandler, err = objectHandler.New(bucketName)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	var authzHandler authz.AuthInterface

	if viper.GetString(config.AUTHENTICATION_TYPE) == "INSECURE" {
		authzHandler = &authz.TestHandler{}
	} else {
		authzHandler, err = authz.InitAuthHandlerFromConf(db)
		if err != nil {
			log.Println(err.Error())
			return nil, err
		}
	}

	commonHandler := database.Common{
		DB:        db,
		S3Handler: objectHandler,
	}

	eventStreamMgmt, err := eventstreaming.New(&database.Read{Common: &commonHandler}, &database.Create{Common: &commonHandler})
	if err != nil {
		log.Println(err.Error())
		return nil, err
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
		ObjectStreamhandler: &database.Streaming{
			Common:            &commonHandler,
			StreamingEndpoint: streamingEndpoint,
			SigningSecret:     streamSigningSecret,
		},
		EventStreamMgmt: eventStreamMgmt,
	}

	return endpoints, nil
}
