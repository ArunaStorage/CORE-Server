package database

import (
	"github.com/ScienceObjectsDB/CORE-Server/models"
	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

func MakeMigrationsStandalone() error {
	db, err := InitDatabaseConnection()
	if err != nil {
		log.Fatalln(err.Error())
	}

	err = MakeMigrationsStandaloneFromDB(db)
	if err != nil {
		log.Fatalln(err.Error())
	}

	return nil
}

func MakeMigrationsStandaloneFromDB(db *gorm.DB) error {
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

	return err
}
