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
		&models.Object{},
		&models.ObjectGroup{},
		&models.Location{},
		&models.Label{},
		&models.User{},
		&models.APIToken{},
		&models.StreamingEntry{},
		&models.StreamGroup{},
		&models.ObjectGroupRevision{},
	)

	if err != nil && err.Error() != "ERROR: duplicate index name: \"idx_users_user_oauth2_id\" (SQLSTATE 42P07)" {
		log.Fatalln(err.Error())
	}

	err = db.AutoMigrate(&models.User{})

	return nil
}
