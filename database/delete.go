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

	var datasetLabels []*models.Label
	var objectGroups []*models.ObjectGroup

	err := crdbgorm.ExecuteTx(context.Background(), handler.DB, nil, func(tx *gorm.DB) error {
		return tx.Transaction(func(tx *gorm.DB) error {
			// Get dataset Labels
			err := tx.Model(&dataset).Association("Labels").Find(&datasetLabels)
			if err != nil {
				log.Println(err.Error())
				return err
			}

			// Preemptively delete object groups registered under the dataset to prevent foreign key violation
			err = tx.Model(&dataset).Association("ObjectGroups").Find(&objectGroups)
			if err != nil {
				log.Println(err.Error())
				return err
			}

			if len(objectGroups) > 0 {
				err = tx.Select(
					"Labels",
					"CurrentObjectGroupRevision",
					"ObjectGroupRevisions",
					"ObjectGroupRevisions.Objects",
					"ObjectGroupRevisions.MetaObjects").Unscoped().Delete(objectGroups).Error
				if err != nil {
					log.Println(err.Error())
					return err
				}
			}

			// Delete Dataset labels
			err = tx.Select(
				"Labels",
				"MetaObjects",
				"ObjectGroups",
				"DatasetVersions").Unscoped().Delete(dataset).Error

			if len(datasetLabels) > 0 {
				err = tx.
					Unscoped().
					Delete(&datasetLabels).Error
			}

			return err
		})

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

	var labels []*models.Label

	err := crdbgorm.ExecuteTx(context.Background(), handler.DB, nil, func(tx *gorm.DB) error {
		return tx.Transaction(func(tx *gorm.DB) error {
			// Get dataset Labels
			err := tx.Model(&version).Association("Labels").Find(&labels)
			if err != nil {
				log.Println(err.Error())
				return err
			}

			err = tx.Select(
				"Labels",
				"ObjectGroupRevisions").Unscoped().Delete(version).Error

			// Delete dangling dataset Label records if available
			if len(labels) > 0 {
				return tx.
					Unscoped().
					Delete(&labels).Error
			}

			return err
		})
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

	var labels []*models.Label

	err := crdbgorm.ExecuteTx(context.Background(), handler.DB, nil, func(tx *gorm.DB) error {
		return tx.Transaction(func(tx *gorm.DB) error {
			// Get project Label records
			err := tx.Model(&project).Association("Labels").Find(&labels)
			if err != nil {
				log.Println(err.Error())
				return err
			}

			// Delete project which should cascade delete
			//   - All elements which are directly associated
			//   - All mapping table elements of many2many associations
			err = tx.
				Select("Users", "Labels", "APIToken", "Datasets").
				Unscoped().
				Delete(project).Error
			if err != nil {
				log.Println(err.Error())
				return err
			}

			// Delete dangling project Label records if available
			if len(labels) > 0 {
				return tx.
					Unscoped().
					Delete(&labels).Error
			}

			return err
		})
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
