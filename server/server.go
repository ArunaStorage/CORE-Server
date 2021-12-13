package server

import (
	"fmt"
	"net"
	"os"

	"github.com/ScienceObjectsDB/CORE-Server/authz"
	"github.com/ScienceObjectsDB/CORE-Server/database"
	"github.com/ScienceObjectsDB/CORE-Server/eventstreaming"
	"github.com/ScienceObjectsDB/CORE-Server/objectstorage"
	"github.com/ScienceObjectsDB/CORE-Server/streamingserver"
	"golang.org/x/sync/errgroup"
	"gorm.io/gorm"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"google.golang.org/grpc"

	services "github.com/ScienceObjectsDB/go-api/api/services/v1"
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
	UseEventStreaming   bool
}

type Server struct {
}

// Starts the gRPC and the data streaming server.
func Run(host string, gRPCPort uint16) error {
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

	var eventStreamMgmt eventstreaming.EventStreamMgmt

	if endpoints.UseEventStreaming {
		eventStreamMgmt, err = eventstreaming.NewNatsEventStreamMgmt(endpoints.ReadHandler)
		if err != nil {
			log.Errorln(err.Error())
			return err
		}
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

	notificationEndpoints, err := NewNotificationEndpoints(endpoints, eventStreamMgmt)
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

	services.RegisterProjectServiceServer(grpcServer, projectEndpoints)
	services.RegisterDatasetServiceServer(grpcServer, datasetEndpoints)
	services.RegisterDatasetObjectsServiceServer(grpcServer, objectEndpoints)
	services.RegisterObjectLoadServiceServer(grpcServer, loadEndpoints)
	services.RegisterUpdateNotificationServiceServer(grpcServer, notificationEndpoints)

	serverErrGrp.Go(func() error {
		return grpcServer.Serve(grpcListener)
	})

	return serverErrGrp.Wait()
}

// Creates the endpoint config based on the provided config.
func createGenericEndpoint() (*Endpoints, error) {
	dbHost := viper.GetString("DB.Host")
	dbPort := viper.GetUint("DB.Port")
	dbName := viper.GetString("DB.Name")
	dbUsername := viper.GetString("DB.Username")

	streamingEndpoint := viper.GetString("Streaming.Endpoint")
	streamSigningSecret := os.Getenv("STREAMINGSIGNSECRET")

	var db *gorm.DB
	var err error

	db, err = database.NewPsqlDB(dbHost, uint64(dbPort), dbUsername, dbName)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	bucketName := viper.GetString("S3.BucketPrefix")

	objectHandler := &objectstorage.S3ObjectStorageHandler{}
	objectHandler, err = objectHandler.New(bucketName)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	authzHandler, err := authz.InitAuthHandlerFromConf(db)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	commonHandler := database.Common{
		DB:        db,
		S3Handler: objectHandler,
	}

	eventNotificationsMgmt, err := eventstreaming.NewNatsEventStreamMgmt(&database.Read{
		Common: &commonHandler,
	})
	if err != nil {
		log.Errorln(err.Error())
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
		EventStreamMgmt: eventNotificationsMgmt,
	}

	return endpoints, nil
}
