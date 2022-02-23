package database

import (
	"context"

	"github.com/ScienceObjectsDB/CORE-Server/models"
	v1storagemodels "github.com/ScienceObjectsDB/go-api/sciobjsdb/api/storage/models/v1"
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

func (update *Update) UpdateStatus(status v1storagemodels.Status, resourceID uuid.UUID, resourceType v1storagemodels.Resource) error {

	var model interface{}

	switch resourceType {
	case v1storagemodels.Resource_RESOURCE_PROJECT:
		model = models.Project{}
	case v1storagemodels.Resource_RESOURCE_DATASET:
		model = models.Dataset{}
	case v1storagemodels.Resource_RESOURCE_OBJECT_GROUP:
		model = models.ObjectGroup{}
	case v1storagemodels.Resource_RESOURCE_OBJECT:
		model = models.Object{}
	case v1storagemodels.Resource_RESOURCE_DATASET_VERSION:
		model = models.DatasetVersion{}
	}

	err := crdbgorm.ExecuteTx(context.Background(), update.DB, nil, func(tx *gorm.DB) error {
		return tx.Model(model).Where("id = ?", resourceID).Update("status", status.String()).Error
	})

	if err != nil {
		log.Error(err.Error())
		return err
	}

	return nil
}
