package database

import (
	"context"
	"errors"
	"time"

	"github.com/cockroachdb/cockroach-go/v2/crdb/crdbgorm"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"

	"github.com/ScienceObjectsDB/CORE-Server/models"
	v1storagemodels "github.com/ScienceObjectsDB/go-api/sciobjsdb/api/storage/models/v1"
)

type Read struct {
	*Common
}

func (read *Read) GetProject(projectID uuid.UUID) (*models.Project, error) {
	project := &models.Project{}
	project.ID = projectID

	err := crdbgorm.ExecuteTx(context.Background(), read.DB, nil, func(tx *gorm.DB) error {
		return tx.Preload("Labels").Preload("Metadata").First(project).Error
	})

	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	return project, nil
}

func (read *Read) GetDataset(datasetID uuid.UUID) (*models.Dataset, error) {
	dataset := &models.Dataset{}
	dataset.ID = datasetID

	err := crdbgorm.ExecuteTx(context.Background(), read.DB, nil, func(tx *gorm.DB) error {
		return tx.Preload("Labels").Preload("Metadata").First(dataset).Error
	})
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	return dataset, nil
}

func (read *Read) GetObjectGroup(objectGroupID uuid.UUID) (*models.ObjectGroup, error) {
	objectGroup := &models.ObjectGroup{}
	objectGroup.ID = objectGroupID
	err := crdbgorm.ExecuteTx(context.Background(), read.DB, nil, func(tx *gorm.DB) error {
		return tx.Preload("Metadata").Preload("Labels").Preload("Objects").First(objectGroup).Error
	})

	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	return objectGroup, nil
}

func (read *Read) GetObjectGroupRevisionsObjects(objectGroupRevisionID uuid.UUID) ([]*models.Object, error) {
	objects := make([]*models.Object, 0)

	err := crdbgorm.ExecuteTx(context.Background(), read.DB, nil, func(tx *gorm.DB) error {
		return tx.Preload("Labels").Preload("Metadata").Where("object_group_revision_id = ?", objectGroupRevisionID).Find(&objects).Error
	})

	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	return objects, nil
}

func (read *Read) GetProjectDatasets(projectID uuid.UUID) ([]*models.Dataset, error) {
	objects := make([]*models.Dataset, 0)

	err := crdbgorm.ExecuteTx(context.Background(), read.DB, nil, func(tx *gorm.DB) error {
		return tx.Preload("Labels").Preload("Metadata").Where("project_id = ?", projectID).Find(&objects).Error
	})

	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	return objects, nil
}

func (read *Read) GetDatasetObjectGroups(datasetID uuid.UUID, page *v1storagemodels.PageRequest) ([]*models.ObjectGroup, error) {
	objectGroups := make([]*models.ObjectGroup, 0)

	if page == nil || page.PageSize == 0 {
		err := crdbgorm.ExecuteTx(context.Background(), read.DB, nil, func(tx *gorm.DB) error {
			return tx.Preload("Objects.Location").Preload("Objects").Preload("Labels").Preload("Metadata").Where("dataset_id = ?", datasetID).Find(&objectGroups).Error
		})
		if err != nil {
			log.Println(err.Error())
			return nil, err
		}
	} else if page != nil && page.LastUuid == "" {
		err := crdbgorm.ExecuteTx(context.Background(), read.DB, nil, func(tx *gorm.DB) error {
			preload := tx.Preload("Objects.Location").Preload("Objects").Preload("Labels").Preload("Metadata")
			return preload.Where("dataset_id = ?", datasetID).Order("id asc").Limit(int(page.PageSize)).Find(&objectGroups).Error
		})

		if err != nil {
			log.Println(err.Error())
			return nil, err
		}
	} else if page != nil && page.LastUuid != "" && page.PageSize > 0 {
		err := crdbgorm.ExecuteTx(context.Background(), read.DB, nil, func(tx *gorm.DB) error {
			preload := tx.Preload("Objects.Location").Preload("Objects").Preload("Labels").Preload("Metadata")
			return preload.Where("dataset_id = ? AND id > ?", datasetID, page.LastUuid).Order("id asc").Limit(int(page.PageSize)).Find(&objectGroups).Error
		})

		if err != nil {
			log.Println(err.Error())
			return nil, err
		}
	} else {
		log.Info("could not parse request")
		return nil, errors.New("could not parse request")
	}

	return objectGroups, nil
}

func (read *Read) GetObject(objectID uuid.UUID) (*models.Object, error) {
	object := models.Object{}
	object.ID = objectID

	err := crdbgorm.ExecuteTx(context.Background(), read.DB, nil, func(tx *gorm.DB) error {
		return tx.Preload("Labels").Preload("Metadata").Preload("Location").First(&object).Error
	})

	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	return &object, nil
}

func (read *Read) GetDatasetVersion(versionID uuid.UUID) (*models.DatasetVersion, error) {
	datasetVersion := &models.DatasetVersion{}
	datasetVersion.ID = versionID

	err := crdbgorm.ExecuteTx(context.Background(), read.DB, nil, func(tx *gorm.DB) error {
		return tx.Preload("Labels").Preload("Metadata").Find(datasetVersion).Error
	})

	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	return datasetVersion, nil
}

func (read *Read) GetDatasetVersions(datasetID uuid.UUID) ([]models.DatasetVersion, error) {
	var datasetVersions []models.DatasetVersion

	err := crdbgorm.ExecuteTx(context.Background(), read.DB, nil, func(tx *gorm.DB) error {
		return tx.Preload("Metadata").Preload("Labels").Where("dataset_id = ?", datasetID).Find(&datasetVersions).Error
	})

	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	return datasetVersions, nil
}

func (read *Read) GetAPIToken(userOAuth2ID uuid.UUID) ([]models.APIToken, error) {
	user := &models.User{}

	err := crdbgorm.ExecuteTx(context.Background(), read.DB, nil, func(tx *gorm.DB) error {
		return tx.Where("user_oauth2_id = ?", userOAuth2ID).Find(user).Error
	})

	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	token := make([]models.APIToken, 0)
	err = crdbgorm.ExecuteTx(context.Background(), read.DB, nil, func(tx *gorm.DB) error {
		return tx.Where("user_uuid = ?", userOAuth2ID).Find(&token).Error
	})

	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	return token, nil
}

func (read *Read) GetDatasetVersionWithObjectGroups(datasetVersionID uuid.UUID, page *v1storagemodels.PageRequest) (*models.DatasetVersion, error) {
	version := &models.DatasetVersion{}
	version.ID = datasetVersionID

	err := crdbgorm.ExecuteTx(context.Background(), read.DB, nil, func(tx *gorm.DB) error {
		return tx.First(version).Error
	})

	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	objectGroupsRefs := make([]*models.ObjectGroup, 0)

	if page == nil || page.PageSize == 0 {
		err := crdbgorm.ExecuteTx(context.Background(), read.DB, nil, func(tx *gorm.DB) error {
			preload := tx.Preload("Objects.Location").Preload("Objects").Preload("Labels").Preload("Metadata").Joins("INNER JOIN dataset_version_object_groups on dataset_version_object_groups.object_group_id=object_groups.id")
			return preload.Where("dataset_version_object_groups.dataset_version_id = ?", datasetVersionID).Find(&objectGroupsRefs).Error
		})
		if err != nil {
			log.Println(err.Error())
			return nil, err
		}
	} else if page != nil && page.LastUuid == "" {
		err := crdbgorm.ExecuteTx(context.Background(), read.DB, nil, func(tx *gorm.DB) error {
			preload := tx.Preload("Objects.Location").Preload("Objects").Preload("Labels").Preload("Metadata").Joins("INNER JOIN dataset_version_object_groups on dataset_version_object_groups.object_group_id=object_groups.id")
			return preload.Where("dataset_version_object_groups.dataset_version_id = ?", datasetVersionID).Order("id asc").Limit(int(page.PageSize)).Find(&objectGroupsRefs).Error
		})

		if err != nil {
			log.Println(err.Error())
			return nil, err
		}
	} else if page != nil && page.LastUuid != "" && page.PageSize > 0 {
		err := crdbgorm.ExecuteTx(context.Background(), read.DB, nil, func(tx *gorm.DB) error {
			preload := tx.Preload("Objects.Location").Preload("Objects").Preload("Labels").Preload("Metadata").Joins("INNER JOIN dataset_version_object_groups on dataset_version_object_groups.object_group_id=object_groups.id")
			return preload.Where("dataset_version_object_groups.dataset_version_id = ? AND id > ?", datasetVersionID, page.LastUuid).Order("id asc").Limit(int(page.PageSize)).Find(&objectGroupsRefs).Error
		})

		if err != nil {
			log.Println(err.Error())
			return nil, err
		}
	} else {
		log.Info("could not parse request")
		return nil, errors.New("could not parse request")
	}

	objectGroups := make([]models.ObjectGroup, len(objectGroupsRefs))
	for i, group := range objectGroupsRefs {
		objectGroups[i] = *group
	}

	version.ObjectGroups = objectGroups

	return version, nil
}

func (read *Read) GetUserProjects(userIDOauth2 string) ([]*models.Project, error) {
	var users []*models.User

	err := crdbgorm.ExecuteTx(context.Background(), read.DB, nil, func(tx *gorm.DB) error {
		return tx.Preload("Project").Where("user_oauth2_id = ?", userIDOauth2).Find(&users).Error
	})

	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	var projects []*models.Project
	for _, user := range users {
		projects = append(projects, &user.Project)
	}

	return projects, nil
}

func (read *Read) GetAllDatasetObjects(datasetID uuid.UUID) ([]*models.Object, error) {
	var objects []*models.Object

	err := crdbgorm.ExecuteTx(context.Background(), read.DB, nil, func(tx *gorm.DB) error {
		return tx.Preload("Location").Where("dataset_id = ?", datasetID).Find(&objects).Error
	})

	if err != nil {
		log.Error(err.Error())
		return nil, err
	}

	return objects, nil
}

func (read *Read) GetAllProjectObjects(projectID uuid.UUID) ([]*models.Object, error) {
	var objects []*models.Object

	err := crdbgorm.ExecuteTx(context.Background(), read.DB, nil, func(tx *gorm.DB) error {
		return tx.Preload("Location").Where("project_id = ?", projectID).Find(&objects).Error
	})

	if err != nil {
		log.Error(err.Error())
		return nil, err
	}

	return objects, nil
}

func (read *Read) GetAllObjectGroupObjects(objectGroupID uuid.UUID) ([]*models.Object, error) {
	var objects []*models.Object

	err := crdbgorm.ExecuteTx(context.Background(), read.DB, nil, func(tx *gorm.DB) error {
		return tx.Preload("Location").Where("object_group_id = ?", objectGroupID).Find(&objects).Error
	})

	if err != nil {
		log.Error(err.Error())
		return nil, err
	}

	return objects, nil
}

func (read *Read) GetAllObjectGroupRevisionObjects(revisionID uuid.UUID) ([]*models.Object, error) {
	var objects []*models.Object

	err := crdbgorm.ExecuteTx(context.Background(), read.DB, nil, func(tx *gorm.DB) error {
		return tx.Preload("Location").Where("object_group_revision_id = ?", revisionID).Find(&objects).Error
	})

	if err != nil {
		log.Error(err.Error())
		return nil, err
	}

	return objects, nil
}

func (read *Read) GetObjectGroupsInDateRange(datasetID uuid.UUID, startDate time.Time, endDate time.Time) ([]*models.ObjectGroup, error) {
	var objectGroups []*models.ObjectGroup
	err := crdbgorm.ExecuteTx(context.Background(), read.DB, nil, func(tx *gorm.DB) error {
		preloadConf := tx.Preload("Metadata").Preload("Labels").Preload("Objects").Preload("Objects.Location").Preload("Objects.Metadata").Preload("Objects.Labels")
		return preloadConf.Where("dataset_id = ? AND generated  BETWEEN ? AND ?", datasetID, startDate, endDate).Find(&objectGroups).Error
	})

	if err != nil {
		log.Error(err.Error())
		return nil, err
	}

	return objectGroups, nil
}

func (read *Read) GetObjectsBatch(ids []uuid.UUID) ([]*models.Object, error) {
	objects := make([]*models.Object, len(ids))
	for i, id := range ids {
		object := &models.Object{}
		object.ID = id
		objects[i] = object
	}

	err := crdbgorm.ExecuteTx(context.Background(), read.DB, nil, func(tx *gorm.DB) error {
		return tx.Preload("Metadata").Preload("Labels").Find(&objects).Error
	})

	if err != nil {
		log.Error(err.Error())
		return nil, err
	}

	return objects, nil
}

// BatchedReads
func (read *Read) GetDatasetObjectGroupsBatches(datasetID uuid.UUID, objectGroupsChan chan []*models.ObjectGroup) error {
	objectGroups := make([]*models.ObjectGroup, 0)
	err := crdbgorm.ExecuteTx(context.Background(), read.DB, nil, func(tx *gorm.DB) error {
		err := tx.Preload("Objects.Location").Preload("Objects").Preload("Labels").Preload("Metadata").Where("dataset_id = ?", datasetID).FindInBatches(&objectGroups, 10000, func(tx *gorm.DB, batch int) error {
			var objectGroupsBatch []*models.ObjectGroup
			objectGroupsBatch = append(objectGroupsBatch, objectGroups...)

			objectGroupsChan <- objectGroupsBatch
			return nil
		}).Error

		return err
	})

	if err != nil {
		log.Error(err.Error())
		return err
	}

	return nil
}

func (read *Read) GetObjectGroupsInDateRangeBatches(datasetID uuid.UUID, startDate time.Time, endDate time.Time, objectGroupsChan chan []*models.ObjectGroup) error {
	var objectGroups []*models.ObjectGroup
	err := crdbgorm.ExecuteTx(context.Background(), read.DB, nil, func(tx *gorm.DB) error {
		preloadConf := read.DB.Preload("Metadata").Preload("Labels").Preload("Objects").Preload("Objects.Location").Preload("Objects.Metadata").Preload("Objects.Labels")
		err := preloadConf.Where("dataset_id = ? AND generated  BETWEEN ? AND ?", datasetID, startDate, endDate).FindInBatches(&objectGroups, 10000, func(tx *gorm.DB, batch int) error {
			var objectGroupsBatch []*models.ObjectGroup
			objectGroupsBatch = append(objectGroupsBatch, objectGroups...)
			objectGroupsChan <- objectGroupsBatch
			return nil
		}).Error
		return err
	})

	if err != nil {
		log.Error(err.Error())
		return err
	}

	return nil
}

func (read *Read) GetObjectGroupsByStatus(objectGroupID []string, status []string) ([]models.ObjectGroup, error) {
	var datasetVersions []models.ObjectGroup

	err := crdbgorm.ExecuteTx(context.Background(), read.DB, nil, func(tx *gorm.DB) error {
		return tx.Preload("Metadata").Preload("Labels").Where("id in ? and status not in ?", objectGroupID, status).Find(&datasetVersions).Error
	})

	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	return datasetVersions, nil
}

func (read *Read) GetStreamGroup(streamGroupID uuid.UUID) (*models.StreamGroup, error) {
	streamGroup := &models.StreamGroup{}
	streamGroup.ID = streamGroupID
	err := crdbgorm.ExecuteTx(context.Background(), read.DB, nil, func(tx *gorm.DB) error {
		return tx.First(streamGroup).Error
	})

	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	return streamGroup, nil
}
