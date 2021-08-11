package database

import (
	"github.com/ScienceObjectsDB/CORE-Server/models"
	log "github.com/sirupsen/logrus"
)

type Update struct {
	*Common
}

func (update *Update) AddUploadID(objectID uint, uploadID string) error {
	if err := update.DB.Model(&models.Object{}).Where("id = ?", objectID).Update("upload_id", uploadID).Error; err != nil {
		log.Println(err.Error())
		return err
	}

	return nil
}
