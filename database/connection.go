package database

import (
	"fmt"
	"os"

	"github.com/ScienceObjectsDB/CORE-Server/config"
	"github.com/ScienceObjectsDB/CORE-Server/models"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type DatabaseType int64

const (
	Undefined DatabaseType = iota
	CockroachDB
	Postgres
)

func InitDatabaseConnection() (*gorm.DB, error) {
	var db *gorm.DB
	var err error

	var databaseType DatabaseType

	databaseTypeString := os.Getenv("DatabaseType")
	if databaseTypeString == "" {
		databaseTypeString = viper.GetString(config.DB_DATABASETYPE)
	}

	switch databaseTypeString {
	case "CockroachDB":
		databaseType = CockroachDB
	case "Postgres":
		databaseType = Postgres
	default:
		databaseType = Undefined
	}

	switch databaseType {
	case Undefined:
		return nil, fmt.Errorf("no valid database was set, please refer to the documentation")
	case CockroachDB:
		db, err = initCockroachConnection()
	case Postgres:
		db, err = initPsqlConnection()
	}

	if err != nil {
		return nil, err
	}

	db = makeMigrations(db)

	return db, nil

}

func initCockroachConnection() (*gorm.DB, error) {
	cockroachUsername := viper.GetString(config.DB_ROACH_USER)
	cockroachHostname := viper.GetString(config.DB_ROACH_HOSTNAME)
	cockroachPort := viper.GetInt(config.DB_ROACH_PORT)
	databasename := viper.GetString(config.DB_ROACH_DATABASENAME)

	password := os.Getenv(config.DB_ROACH_PASSWORDENVVAR)
	dsn := fmt.Sprintf("postgres://%v:%v@%v:%v/%v?sslmode=prefer&TimeZone=Europe/Berlin", cockroachUsername, password, cockroachHostname, cockroachPort, databasename)

	if password == "" {
		dsn = fmt.Sprintf("postgres://%v@%v:%v/%v?sslmode=disable&TimeZone=Europe/Berlin", cockroachUsername, cockroachHostname, cockroachPort, databasename)
	}

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: NewGormLogger(),
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	return db, nil
}

func initPsqlConnection() (*gorm.DB, error) {
	postgresUsername := viper.GetString(config.DB_POSTGRES_USER)
	postgresHostname := viper.GetString(config.DB_POSTGRES_HOSTNAME)
	postgresPort := viper.GetInt(config.DB_POSTGRES_PORT)
	databasename := viper.GetString(config.DB_POSTGRES_DATABASENAME)

	psqlPW := os.Getenv(viper.GetString(config.DB_POSTGRES_PASSWORDENVVAR))
	dsn := fmt.Sprintf("postgres://%v:%v@%v:%v/%v?sslmode=prefer&TimeZone=Europe/Berlin", postgresUsername, psqlPW, postgresHostname, postgresPort, databasename)

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: NewGormLogger(),
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	return db, nil
}

func makeMigrations(db *gorm.DB) *gorm.DB {
	err := db.AutoMigrate(
		&models.Project{},
		&models.Dataset{},
		&models.DatasetVersion{},
		&models.ObjectGroup{},
		&models.Object{},
		&models.Location{},
		&models.Metadata{},
		&models.Label{},
		&models.APIToken{},
		&models.User{},
		&models.StreamingEntry{},
		&models.StreamGroup{},
	)

	if err != nil {
		log.Fatalln(err.Error())
	}

	return db
}
