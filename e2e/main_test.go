package e2e

import (
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/ScienceObjectsDB/CORE-Server/authz"
	"github.com/ScienceObjectsDB/CORE-Server/database"
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
	init_test_endpoints()
	code := m.Run()
	os.Exit(code)
}

func init_test_endpoints() {
	viper.SetConfigName("config") // name of config file (without extension)
	viper.SetConfigType("yaml")   // REQUIRED if the config file does not have the extension in the name

	viper.AddConfigPath("./config")

	err := viper.ReadInConfig() // Find and read the config file
	if err != nil {             // Handle errors reading the config file
		panic(fmt.Errorf("fatal error config file: %w", err))
	}

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
		AuthzHandler: authzHandler,
	}

	serverEndpoints := &ServerEndpointsTest{
		project: &server.ProjectEndpoints{Endpoints: endpoints},
		dataset: &server.DatasetEndpoints{Endpoints: endpoints},
		object:  &server.ObjectServerEndpoints{Endpoints: endpoints},
		load:    &server.LoadEndpoints{Endpoints: endpoints},
	}

	ServerEndpoints = serverEndpoints
}
