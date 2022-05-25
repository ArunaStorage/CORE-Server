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
		return tx.Preload("Labels").First(project).Error
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
		return tx.Preload("Labels").Preload("MetaObjects").First(dataset).Error
	})
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	return dataset, nil
}

func (read *Read) GetObjectGroupRevision(objectGroupRevisionsID uuid.UUID) (*models.ObjectGroupRevision, error) {
	objectGroupRevision := &models.ObjectGroupRevision{}
	objectGroupRevision.ID = objectGroupRevisionsID
	err := crdbgorm.ExecuteTx(context.Background(), read.DB, nil, func(tx *gorm.DB) error {
		return tx.Preload("MetaObjects.DefaultLocation").Preload("MetaObjects.Locations").Preload("Objects.DefaultLocation").Preload("Objects.Locations").Preload("Labels").Preload("Objects").Preload("MetaObjects").First(objectGroupRevision).Error
	})

	if err != nil {
		log.Errorln(err.Error())
		return nil, err
	}

	return objectGroupRevision, nil
}

func (read *Read) GetObjectGroup(objectGroupID uuid.UUID) (*models.ObjectGroup, error) {
	objectGroup := &models.ObjectGroup{}
	objectGroup.ID = objectGroupID

	err := crdbgorm.ExecuteTx(context.Background(), read.DB, nil, func(tx *gorm.DB) error {
		preloads := tx.Preload("CurrentObjectGroupRevision.MetaObjects.Locations").Preload("CurrentObjectGroupRevision.MetaObjects.DefaultLocation").Preload("CurrentObjectGroupRevision.Objects.Locations").Preload("CurrentObjectGroupRevision.Objects.DefaultLocation").Preload("CurrentObjectGroupRevision").Preload("CurrentObjectGroupRevision.Objects").Preload("CurrentObjectGroupRevision.MetaObjects").Preload("CurrentObjectGroupRevision.Labels")
		return preloads.First(objectGroup).Error
	})

	if err != nil {
		log.Errorln(err.Error())
		return nil, err
	}

	return objectGroup, nil
}

func (read *Read) GetObjectGroupRevisionsObjects(objectGroupRevisionID uuid.UUID) ([]*models.Object, error) {
	objects := make([]*models.Object, 0)

	err := crdbgorm.ExecuteTx(context.Background(), read.DB, nil, func(tx *gorm.DB) error {
		return tx.Preload("Labels").Where("object_group_revision_id = ?", objectGroupRevisionID).Find(&objects).Error
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
		return tx.Preload("Labels").Where("project_id = ?", projectID).Find(&objects).Error
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
			preload := tx.Preload("CurrentObjectGroupRevision").Preload("CurrentObjectGroupRevision.Objects").Preload("CurrentObjectGroupRevision.Labels").Preload("CurrentObjectGroupRevision.MetaObjects")
			return preload.Where("dataset_id = ?", datasetID).Find(&objectGroups).Error
		})
		if err != nil {
			log.Println(err.Error())
			return nil, err
		}
	} else if page != nil && page.LastUuid == "" {
		err := crdbgorm.ExecuteTx(context.Background(), read.DB, nil, func(tx *gorm.DB) error {
			preload := tx.Preload("CurrentObjectGroupRevision").Preload("CurrentObjectGroupRevision.Objects").Preload("CurrentObjectGroupRevision.Labels").Preload("CurrentObjectGroupRevision.MetaObjects")
			return preload.Where("dataset_id = ?", datasetID).Order("id asc").Limit(int(page.PageSize)).Find(&objectGroups).Error
		})

		if err != nil {
			log.Println(err.Error())
			return nil, err
		}
	} else if page != nil && page.LastUuid != "" && page.PageSize > 0 {
		err := crdbgorm.ExecuteTx(context.Background(), read.DB, nil, func(tx *gorm.DB) error {
			preload := tx.Preload("CurrentObjectGroupRevision.Objects.Locations").Preload("CurrentObjectGroupRevision").Preload("CurrentObjectGroupRevision.Objects").Preload("CurrentObjectGroupRevision.Labels").Preload("CurrentObjectGroupRevision.MetaObjects.Locations")
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
		return tx.Preload("Labels").Preload("Locations").Preload("DefaultLocation").First(&object).Error
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
		return tx.Preload("Labels").Find(datasetVersion).Error
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
		return tx.Preload("Labels").Where("dataset_id = ?", datasetID).Find(&datasetVersions).Error
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
		log.Errorln(err.Error())
		return nil, err
	}

	objectGroupsRevisionRefs := make([]*models.ObjectGroupRevision, 0)

	if page == nil || page.PageSize == 0 {
		err := crdbgorm.ExecuteTx(context.Background(), read.DB, nil, func(tx *gorm.DB) error {
			preload := tx.Preload("Objects.Locations").Preload("Objects.DefaultLocation").Preload("Objects").Preload("Labels").Joins("INNER JOIN dataset_version_object_group_revisions on dataset_version_object_group_revisions.object_group_revision_id=object_group_revisions.id")
			return preload.Where("dataset_version_object_group_revisions.dataset_version_id = ?", datasetVersionID).Find(&objectGroupsRevisionRefs).Error
		})
		if err != nil {
			log.Errorln(err.Error())
			return nil, err
		}
	} else if page != nil && page.LastUuid == "" {
		err := crdbgorm.ExecuteTx(context.Background(), read.DB, nil, func(tx *gorm.DB) error {
			preload := tx.Preload("Objects.Locations").Preload("Objects.DefaultLocation").Preload("Objects").Preload("Labels").Joins("INNER JOIN dataset_version_object_group_revisions on dataset_version_object_group_revisions.object_group_revision_id=object_group_revisions.id")
			return preload.Where("dataset_version_object_group_revisions.dataset_version_id = ?", datasetVersionID).Order("id asc").Limit(int(page.PageSize)).Find(&objectGroupsRevisionRefs).Error
		})

		if err != nil {
			log.Errorln(err.Error())
			return nil, err
		}
	} else if page != nil && page.LastUuid != "" && page.PageSize > 0 {
		err := crdbgorm.ExecuteTx(context.Background(), read.DB, nil, func(tx *gorm.DB) error {
			preload := tx.Preload("Objects.Locations").Preload("Objects.DefaultLocation").Preload("Objects").Preload("Labels").Joins("INNER JOIN dataset_version_object_group_revisions on dataset_version_object_group_revisions.object_group_revision_id=object_group_revisions.id")
			return preload.Where("dataset_version_object_group_revisions.dataset_version_id = ? AND id > ?", datasetVersionID, page.LastUuid).Order("id asc").Limit(int(page.PageSize)).Find(&objectGroupsRevisionRefs).Error
		})

		if err != nil {
			log.Errorln(err.Error())
			return nil, err
		}
	} else {
		log.Info("could not parse request")
		return nil, errors.New("could not parse request")
	}

	objectGroupRevisions := make([]models.ObjectGroupRevision, len(objectGroupsRevisionRefs))
	for i, group := range objectGroupsRevisionRefs {
		objectGroupRevisions[i] = *group
	}

	version.ObjectGroupRevisions = objectGroupRevisions

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
		return tx.Where("dataset_id = ?", datasetID).Find(&objects).Error
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
		return tx.Where("project_id = ?", projectID).Find(&objects).Error
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
		return tx.Where("object_group_id = ?", objectGroupID).Find(&objects).Error
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
		return tx.Preload("Locations").Preload("DefaultLocation").Where("object_group_revision_id = ?", revisionID).Find(&objects).Error
	})

	if err != nil {
		log.Error(err.Error())
		return nil, err
	}

	return objects, nil
}

func (read *Read) GetObjectGroupsInDateRange(datasetID uuid.UUID, startDate time.Time, endDate time.Time) ([]*models.ObjectGroupRevision, error) {
	var objectGroupRevisions []*models.ObjectGroupRevision
	err := crdbgorm.ExecuteTx(context.Background(), read.DB, nil, func(tx *gorm.DB) error {
		preloadConf := tx.Preload("Labels").Preload("Objects").Preload("Objects.Labels").Preload("Objects.Locations").Preload("Objects.DefaultLocation")
		return preloadConf.Where("dataset_id = ? AND generated  BETWEEN ? AND ?", datasetID, startDate, endDate).Find(&objectGroupRevisions).Error
	})

	if err != nil {
		log.Error(err.Error())
		return nil, err
	}

	return objectGroupRevisions, nil
}

func (read *Read) GetObjectsBatch(ids []uuid.UUID) ([]*models.Object, error) {
	objects := make([]*models.Object, len(ids))
	for i, id := range ids {
		object := &models.Object{}
		object.ID = id
		objects[i] = object
	}

	err := crdbgorm.ExecuteTx(context.Background(), read.DB, nil, func(tx *gorm.DB) error {
		return tx.Preload("Labels").Find(&objects).Error
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
		err := tx.Preload("CurrentObjectGroupRevision.Objects.Locations").Preload("CurrentObjectGroupRevision").Preload("CurrentObjectGroupRevision.Objects").Preload("CurrentObjectGroupRevision.Labels").Where("dataset_id = ?", datasetID).FindInBatches(&objectGroups, 10000, func(tx *gorm.DB, batch int) error {
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
		preloadConf := read.DB.Preload("Labels").Preload("Objects").Preload("Objects.Labels").Preload("Objects.Locations").Preload("Objects.DefaultLocation")
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

func (read *Read) GetObjectGroupRevisionsByStatus(objectGroupID []string, status []string) ([]models.ObjectGroupRevision, error) {
	var datasetVersions []models.ObjectGroupRevision

	err := crdbgorm.ExecuteTx(context.Background(), read.DB, nil, func(tx *gorm.DB) error {
		return tx.Preload("Labels").Where("id in ? and status not in ?", objectGroupID, status).Find(&datasetVersions).Error
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
