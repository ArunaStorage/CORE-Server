package e2e

import (
	"fmt"
	"os"
	"testing"

	log "github.com/sirupsen/logrus"

	"github.com/ScienceObjectsDB/CORE-Server/authz"
	"github.com/ScienceObjectsDB/CORE-Server/config"
	"github.com/ScienceObjectsDB/CORE-Server/database"
	"github.com/ScienceObjectsDB/CORE-Server/eventstreaming"
	"github.com/ScienceObjectsDB/CORE-Server/objectstorage"
	"github.com/ScienceObjectsDB/CORE-Server/server"
	"github.com/spf13/viper"
)

type ServerEndpointsTest struct {
	project      *server.ProjectEndpoints
	dataset      *server.DatasetEndpoints
	object       *server.ObjectServerEndpoints
	load         *server.LoadEndpoints
	notification *server.NotificationEndpoints
}

var ServerEndpoints = &ServerEndpointsTest{}

func TestMain(m *testing.M) {
	log.SetReportCaller(true)
	init_test_endpoints()
	log.SetLevel(log.ErrorLevel)
	//local_init_test_endpoints()
	code := m.Run()
	os.Exit(code)
}

func init_test_endpoints() {
	viper.SetConfigName("config") // name of config file (without extension)
	viper.SetConfigType("yaml")   // REQUIRED if the config file does not have the extension in the name

	_, e2eComposeVar := os.LookupEnv("E2E_TEST_COMPOSE")

	if e2eComposeVar {
		viper.AddConfigPath("./config_compose")
	} else {
		viper.AddConfigPath("./config")
		os.Setenv("AWS_ACCESS_KEY_ID", "minioadmin")
		os.Setenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
	}

	config.SetDefaults()

	err := viper.ReadInConfig() // Find and read the config file
	if err != nil {             // Handle errors reading the config file
		panic(fmt.Errorf("fatal error config file: %w", err))
	}

	os.Setenv("PSQL_PASSWORD", "test123")

	db, err := database.InitDatabaseConnection()
	if err != nil {
		log.Fatalln(err.Error())
	}

	err = database.MakeMigrationsStandaloneFromDB(db)
	if err != nil {
		log.Fatalln(err.Error())
	}

	bucketName := viper.GetString(config.S3_BUCKET_PREFIX)

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

	eventMgmt, err := eventstreaming.New(&database.Read{Common: &commonHandler}, &database.Create{Common: &commonHandler})
	if err != nil {
		log.Fatalln(err.Error())
	}

	err = eventMgmt.EnableTestMode()
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
		StatsHandler: &database.Stats{
			Common: &commonHandler,
		},
		AuthzHandler:    authzHandler,
		EventStreamMgmt: eventMgmt,
	}

	serverEndpoints := &ServerEndpointsTest{
		project:      &server.ProjectEndpoints{Endpoints: endpoints},
		dataset:      &server.DatasetEndpoints{Endpoints: endpoints},
		object:       &server.ObjectServerEndpoints{Endpoints: endpoints},
		load:         &server.LoadEndpoints{Endpoints: endpoints},
		notification: &server.NotificationEndpoints{Endpoints: endpoints},
	}

	ServerEndpoints = serverEndpoints
}
