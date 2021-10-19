package database

import (
	"github.com/ScienceObjectsDB/CORE-Server/models"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

type Update struct {
	*Common
}

func (update *Update) AddUploadID(objectID uuid.UUID, uploadID string) error {
	if err := update.DB.Model(&models.Object{}).Where("id = ?", objectID).Update("upload_id", uploadID).Error; err != nil {
		log.Println(err.Error())
		return err
	}

	return nil
}

func (update *Update) UpdateMetadata() error {
	return nil
}

func (update *Update) UpdateLabels() error {
	return nil
}
