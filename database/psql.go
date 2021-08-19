package database

import (
	"fmt"
	"os"

	log "github.com/sirupsen/logrus"

	"github.com/ScienceObjectsDB/CORE-Server/models"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func NewPsqlDB(host string, port uint64, username string, dbName string) (*gorm.DB, error) {
	psqlPW := os.Getenv("PSQL_PASSWORD")

	dsn := fmt.Sprintf("host=%v user=%v password=%v dbname=%v port=%v sslmode=prefer TimeZone=Europe/Berlin", host, username, psqlPW, dbName, port)

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	db = makeMigrations(db)

	return db, nil
}

func NewPsqlDBCITest() (*gorm.DB, error) {
	dsn := "postgres://root@cockroach:26257/defaultdb?sslmode=disable"

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	db = makeMigrations(db)

	return db, nil
}

func makeMigrations(db *gorm.DB) *gorm.DB {
	db.AutoMigrate(
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
	)

	return db
}
