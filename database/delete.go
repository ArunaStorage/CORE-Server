package database

import (
	"context"

	"github.com/ScienceObjectsDB/CORE-Server/models"
	"github.com/cockroachdb/cockroach-go/v2/crdb/crdbgorm"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

type Delete struct {
	*Common
}

func (handler *Delete) DeleteObjectGroup(objectGroupID uuid.UUID) error {
	objectGroup := &models.ObjectGroup{}
	objectGroup.ID = objectGroupID

	err := crdbgorm.ExecuteTx(context.Background(), handler.DB, nil, func(tx *gorm.DB) error {
		return tx.Select("Labels", "Objects").Unscoped().Delete(objectGroup).Error
	})

	if err != nil {
		log.Println(err.Error())
		return err
	}
	return nil
}

func (handler *Delete) DeleteDataset(datasetID uuid.UUID) error {
	dataset := &models.Dataset{}
	dataset.ID = datasetID

	err := crdbgorm.ExecuteTx(context.Background(), handler.DB, nil, func(tx *gorm.DB) error {
		return tx.Select("Labels", "Objects", "ObjectGroups", "DatasetVersion", "ObjectsGroups.Objects").Unscoped().Delete(dataset).Error
	})

	if err != nil {
		log.Println(err.Error())
		return err
	}

	return nil
}

func (handler *Delete) DeleteDatasetVersion(datasetVersionID uuid.UUID) error {
	version := &models.DatasetVersion{}
	version.ID = datasetVersionID

	err := crdbgorm.ExecuteTx(context.Background(), handler.DB, nil, func(tx *gorm.DB) error {
		return tx.Select("Labels").Unscoped().Delete(version).Error
	})

	if err != nil {
		log.Println(err.Error())
		return err
	}

	return nil
}

func (handler *Delete) DeleteProject(projectID uuid.UUID) error {
	project := &models.Project{}
	project.ID = projectID

	err := crdbgorm.ExecuteTx(context.Background(), handler.DB, nil, func(tx *gorm.DB) error {
		return tx.Select("Labels", "User", "APIToken", "Datasets", "Datasets.Objects").Unscoped().Delete(project).Error
	})

	if err != nil {
		log.Println(err.Error())
		return err
	}

	return nil
}

func (handler *Delete) DeleteAPIToken(tokenID uuid.UUID) error {
	token := &models.APIToken{}
	token.ID = tokenID

	err := crdbgorm.ExecuteTx(context.Background(), handler.DB, nil, func(tx *gorm.DB) error {
		return handler.DB.Delete(token).Error
	})

	if err != nil {
		log.Println(err.Error())
		return err
	}

	return nil
}
