package database

import (
	"fmt"
	"os"

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

	databaseTypeEnvVar := os.Getenv("DatabaseType")
	if databaseTypeEnvVar != "" {
		switch databaseTypeEnvVar {
		case "CockroachDB":
			databaseType = CockroachDB
		case "Postgres":
			databaseType = Postgres
		}
	} else {
		if viper.IsSet("DB.Cockroach") {
			databaseType = CockroachDB
		} else if viper.IsSet("DB.Postgres") {
			db, err = initPsqlConnection()
		}
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
	cockroachUsername := viper.GetString("DB.Cockroach.Username")
	cockroachHostname := viper.GetString("DB.Cockroach.Hostname")
	cockroachPort := viper.GetInt("DB.Cockroach.Port")
	databasename := viper.GetString("DB.Cockroach.Databasename")

	password := os.Getenv("PSQL_PASSWORD")
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
	postgresUsername := viper.GetString("DB.Postgres.Username")
	postgresHostname := viper.GetString("DB.Postgres.Hostname")
	postgresPort := viper.GetInt("DB.Postgres.Port")
	databasename := viper.GetString("DB.Postgres.Databasename")

	psqlPW := os.Getenv("PSQL_PASSWORD")
	dsn := fmt.Sprintf("postgres://%v:%v@%v:%v/%v?sslmode=prefer&TimeZone=Europe/Berlin", postgresUsername, psqlPW, postgresHostname, postgresPort, databasename)

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: NewGormLogger(),
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	log.Println(dsn)

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
	)

	if err != nil {
		log.Fatalln(err.Error())
	}

	return db
}
