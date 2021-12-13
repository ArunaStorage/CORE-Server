package e2e

import (
	"fmt"
	"os"
	"testing"

	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"

	"github.com/ScienceObjectsDB/CORE-Server/authz"
	"github.com/ScienceObjectsDB/CORE-Server/database"
	"github.com/ScienceObjectsDB/CORE-Server/eventstreaming"
	"github.com/ScienceObjectsDB/CORE-Server/objectstorage"
	"github.com/ScienceObjectsDB/CORE-Server/server"
	"github.com/spf13/viper"
)

type ServerEndpointsTest struct {
	project *server.ProjectEndpoints
	dataset *server.DatasetEndpoints
	object  *server.ObjectServerEndpoints
	load    *server.LoadEndpoints
}

var ServerEndpoints = &ServerEndpointsTest{}

func TestMain(m *testing.M) {
	log.SetReportCaller(true)

	init_test_endpoints()
	//local_init_test_endpoints()
	code := m.Run()
	os.Exit(code)
}

func local_init_test_endpoints() {
	viper.SetConfigName("config") // name of config file (without extension)
	viper.SetConfigType("yaml")   // REQUIRED if the config file does not have the extension in the name

	viper.AddConfigPath("/home/marius/Code/ScienceObjectsDB/CORE-Server/config/local")

	err := viper.ReadInConfig() // Find and read the config file
	if err != nil {             // Handle errors reading the config file
		panic(fmt.Errorf("fatal error config file: %w", err))
	}

	os.Setenv("PSQL_PASSWORD", "test123")

	dbHost := viper.GetString("DB.Host")
	dbPort := viper.GetUint("DB.Port")
	dbName := viper.GetString("DB.Name")
	dbUsername := viper.GetString("DB.Username")

	db, err := database.NewPsqlDB(dbHost, uint64(dbPort), dbUsername, dbName)
	if err != nil {
		log.Fatalln(err.Error())
	}

	bucketName := viper.GetString("S3.Bucket")

	objectHandler := &objectstorage.S3ObjectStorageHandler{}
	objectHandler, err = objectHandler.New(bucketName)
	if err != nil {
		log.Fatalln(err.Error())
	}

	commonHandler := database.Common{
		DB:        db,
		S3Handler: objectHandler,
	}

	authzHandler := &authz.TestHandler{}

	eventMgmt, err := eventstreaming.NewNatsEventStreamMgmt(&database.Read{
		Common: &commonHandler,
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	_, err = eventMgmt.JetStreamManager.AddStream(&nats.StreamConfig{Name: "UPDATES", Description: "TEST", Subjects: []string{"UPDATES"}})
	if err != nil {
		log.Fatalln(err.Error())
	}

	endpoints := &server.Endpoints{
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
		AuthzHandler:      authzHandler,
		EventStreamMgmt:   eventMgmt,
		UseEventStreaming: true,
	}

	serverEndpoints := &ServerEndpointsTest{
		project: &server.ProjectEndpoints{Endpoints: endpoints},
		dataset: &server.DatasetEndpoints{Endpoints: endpoints},
		object:  &server.ObjectServerEndpoints{Endpoints: endpoints},
		load:    &server.LoadEndpoints{Endpoints: endpoints},
	}

	ServerEndpoints = serverEndpoints
}

func init_test_endpoints() {
	viper.SetConfigName("config") // name of config file (without extension)
	viper.SetConfigType("yaml")   // REQUIRED if the config file does not have the extension in the name

	viper.AddConfigPath("./config")

	err := viper.ReadInConfig() // Find and read the config file
	if err != nil {             // Handle errors reading the config file
		panic(fmt.Errorf("fatal error config file: %w", err))
	}

	db, err := database.NewPsqlDBCITest()
	if err != nil {
		log.Fatalln(err.Error())
	}

	bucketName := viper.GetString("S3.BucketPrefix")

	objectHandler := &objectstorage.S3ObjectStorageHandler{}
	objectHandler, err = objectHandler.New(bucketName)
	if err != nil {
		log.Fatalln(err.Error())
	}

	commonHandler := database.Common{
		DB:        db,
		S3Handler: objectHandler,
	}

	authzHandler := &authz.TestHandler{}

	eventMgmt, err := eventstreaming.NewNatsEventStreamMgmt(&database.Read{
		Common: &commonHandler,
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	_, err = eventMgmt.JetStreamManager.AddStream(&nats.StreamConfig{Name: "UPDATES", Description: "TEST", Subjects: []string{"UPDATES.*", "UPDATES.*.*", "UPDATES.*.*.*"}})
	if err != nil {
		log.Fatalln(err.Error())
	}

	endpoints := &server.Endpoints{
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
		AuthzHandler:      authzHandler,
		EventStreamMgmt:   eventMgmt,
		UseEventStreaming: true,
	}

	serverEndpoints := &ServerEndpointsTest{
		project: &server.ProjectEndpoints{Endpoints: endpoints},
		dataset: &server.DatasetEndpoints{Endpoints: endpoints},
		object:  &server.ObjectServerEndpoints{Endpoints: endpoints},
		load:    &server.LoadEndpoints{Endpoints: endpoints},
	}

	ServerEndpoints = serverEndpoints
}
