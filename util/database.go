package util

import (
	"crypto/rand"

	log "github.com/sirupsen/logrus"

	"github.com/ScienceObjectsDB/CORE-Server/models"
	"gorm.io/gorm"
)

func MakeMigrations(db *gorm.DB) error {
	err := db.AutoMigrate(&models.Project{}, &models.Dataset{}, &models.User{}, &models.UserRight{}, &models.APITokenRight{}, &models.ObjectGroup{}, &models.Object{}, &models.Location{}, &models.APIToken{}, &models.DatasetVersion{})
	if err != nil {
		log.Println(err.Error())
		return err
	}

	return nil
}

func GenerateRandomString(n int) ([]byte, error) {
	b := make([]byte, n)
	_, err := rand.Reader.Read(b)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}
	return b, nil
}
