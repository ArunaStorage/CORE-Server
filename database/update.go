package database

import (
	"context"

	"github.com/ScienceObjectsDB/CORE-Server/models"
	"github.com/cockroachdb/cockroach-go/v2/crdb/crdbgorm"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

type Update struct {
	*Common
}

// Adds an upload id to an object for multipart uploads
func (update *Update) AddUploadID(objectID uuid.UUID, uploadID string) error {
	err := crdbgorm.ExecuteTx(context.Background(), update.DB, nil, func(tx *gorm.DB) error {
		return tx.Model(&models.Object{}).Where("id = ?", objectID).Update("upload_id", uploadID).Error
	})

	if err != nil {
		log.Error(err.Error())
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
