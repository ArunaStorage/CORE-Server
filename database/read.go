package database

import (
	"context"
	"errors"
	"time"

	"github.com/ScienceObjectsDB/CORE-Server/models"
	"github.com/cockroachdb/cockroach-go/v2/crdb/crdbgorm"
	"github.com/google/uuid"
	"gorm.io/gorm"

	v1storagemodels "github.com/ScienceObjectsDB/go-api/sciobjsdb/api/storage/models/v1"
	log "github.com/sirupsen/logrus"
)

type Read struct {
	*Common
}

func (read *Read) GetProject(projectID uuid.UUID) (*models.Project, error) {
	project := &models.Project{}
	project.ID = projectID

	err := crdbgorm.ExecuteTx(context.Background(), read.DB, nil, func(tx *gorm.DB) error {
		return tx.
			Preload("Users").
			Preload("Labels").
			Preload("APIToken").
			Preload("Datasets").
			First(project).Error
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
		return tx.
			Preload("Project").
			Preload("Labels").
			Preload("MetaObjects").
			// ObjectGroups and DatasetVersions should be fetched on demand
			First(dataset).Error
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
		return tx.
			Preload("Project").
			Preload("Dataset").
			Preload("Labels").
			// DatasetVersions should be fetched on demand
			Preload("Objects").
			Preload("MetaObjects").
			First(objectGroupRevision).Error
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
		return tx.
			Preload("Project").
			Preload("Dataset").
			Preload("CurrentObjectGroupRevision.Objects").
			Preload("CurrentObjectGroupRevision.MetaObjects").
			Preload("CurrentObjectGroupRevision.Labels").
			// ObjectGroupRevisions should be fetched on demand
			First(objectGroup).Error
	})

	if err != nil {
		log.Errorln(err.Error())
		return nil, err
	}

	return objectGroup, nil
}


func (read *Read) GetProjectDatasets(projectID uuid.UUID) ([]*models.Dataset, error) {
	datasets := make([]*models.Dataset, 0)

	err := crdbgorm.ExecuteTx(context.Background(), read.DB, nil, func(tx *gorm.DB) error {
		return tx.
			Preload("Project").
			Preload("Labels").
			Preload("MetaObjects").
			Where("project_id = ?", projectID).
			Find(&datasets).Error
	})

	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	return objects, nil
}

func (read *Read) GetDatasetObjectGroups(datasetID uuid.UUID, page *v1storagemodels.PageRequest) ([]*models.ObjectGroup, error) {
	objectGroups := make([]*models.ObjectGroup, 0)

		err := crdbgorm.ExecuteTx(context.Background(), read.DB, nil, func(tx *gorm.DB) error {
		preload := tx.
			Preload("Project").
			Preload("Dataset").
			Preload("CurrentObjectGroupRevision").
			Preload("CurrentObjectGroupRevision.Labels").
			Preload("CurrentObjectGroupRevision.Objects").
			Preload("CurrentObjectGroupRevision.MetaObjects")

		if page == nil || page.PageSize == 0 {
			return preload.
				Where("dataset_id = ?", datasetID).
				Find(&objectGroups).Error

	} else if page != nil && page.LastUuid == "" {
			return preload.
				Where("dataset_id = ?", datasetID).
				Order("id asc").
				Limit(int(page.PageSize)).
				Find(&objectGroups).Error

	} else if page != nil && page.LastUuid != "" && page.PageSize > 0 {
			return preload.
				Where("dataset_id = ? AND id > ?", datasetID, page.LastUuid).
				Order("id asc").
				Limit(int(page.PageSize)).
				Find(&objectGroups).Error

		} else {
			log.Info("could not parse request")
			return errors.New("could not parse request")
		}
		})

		if err != nil {
			log.Println(err.Error())
			return nil, err
		}
	return objectGroups, nil
}

func (read *Read) GetObject(objectID uuid.UUID) (*models.Object, error) {
	object := models.Object{}
	object.ID = objectID

	err := crdbgorm.ExecuteTx(context.Background(), read.DB, nil, func(tx *gorm.DB) error {
		return tx.
			Preload("Project").
			Preload("Dataset").
			Preload("Labels").
			Preload("Locations").
			Preload("DefaultLocation").
			First(&object).Error
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
		return tx.
			Preload("Project").
			Preload("Dataset").
			Preload("Labels").
			// nObjectGroupRevisions should be fetched on demand
			First(datasetVersion).Error
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
		return tx.
			Preload("Project").
			Preload("Dataset").
			Preload("Labels").
			// ObjectGroupRevisions should be fetched on demand
			Where("dataset_id = ?", datasetID).
			Find(&datasetVersions).Error
	})

	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	return datasetVersions, nil
}

func (read *Read) GetAPIToken(userOAuth2ID uuid.UUID) ([]models.APIToken, error) {
	user := &models.User{}
	token := make([]models.APIToken, 0)

	err := crdbgorm.ExecuteTx(context.Background(), read.DB, nil, func(tx *gorm.DB) error {
		err := tx.
			Preload("Project").
			Where("user_oauth2_id = ?", userOAuth2ID).
			Find(user).Error

	if err != nil {
		log.Println(err.Error())
			return err
	}

		return tx.
			Preload("Project").
			Where("user_uuid = ?", userOAuth2ID).
			Find(&token).Error
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

	objectGroupsRevisionRefs := make([]*models.ObjectGroupRevision, 0)

	err := crdbgorm.ExecuteTx(context.Background(), read.DB, nil, func(tx *gorm.DB) error {
		err := tx.First(version).Error

	if err != nil {
		log.Errorln(err.Error())
			return err
	}

		preload := tx.
			Preload("Labels").
			Preload("Objects").
			Preload("Objects.Labels").
			Preload("Objects.Locations").
			Preload("Objects.DefaultLocation").
			Preload("MetaObjects").
			Preload("MetaObjects.Labels").
			Preload("MetaObjects.Locations").
			Preload("MetaObjects.DefaultLocation").
			Joins("INNER JOIN dataset_version_object_group_revisions on dataset_version_object_group_revisions.object_group_revision_id=object_group_revisions.id")

	if page == nil || page.PageSize == 0 {
			return preload.
				Where("dataset_version_object_group_revisions.dataset_version_id = ?", datasetVersionID).
				Find(&objectGroupsRevisionRefs).Error

	} else if page != nil && page.LastUuid == "" {
			return preload.
				Where("dataset_version_object_group_revisions.dataset_version_id = ?", datasetVersionID).
				Order("id asc").
				Limit(int(page.PageSize)).
				Find(&objectGroupsRevisionRefs).Error

		} else if page != nil && page.LastUuid != "" && page.PageSize > 0 {
			return preload.
				Where("dataset_version_object_group_revisions.dataset_version_id = ? AND id > ?", datasetVersionID, page.LastUuid).
				Order("id asc").
				Limit(int(page.PageSize)).
				Find(&objectGroupsRevisionRefs).Error

		} else {
			log.Info("could not parse request")
			return errors.New("could not parse request")
		}

		})

		if err != nil {
			log.Errorln(err.Error())
			return nil, err
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
		return tx.
			Preload("Project").
			Where("user_oauth2_id = ?", userIDOauth2).
			Find(&users).Error
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

func (read *Read) GetProjectUsers(projectID uuid.UUID) ([]*models.User, error) {
	var users []*models.User

	err := crdbgorm.ExecuteTx(context.Background(), read.DB, nil, func(tx *gorm.DB) error {
		return tx.
			Where("project_id = ?", projectID).
			Find(&users).Error
	})

	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	return users, nil
}

func (read *Read) GetAllDatasetObjects(datasetID uuid.UUID) ([]*models.Object, error) {
	var objects []*models.Object

	err := crdbgorm.ExecuteTx(context.Background(), read.DB, nil, func(tx *gorm.DB) error {
		return tx.
			Preload("Project").
			Preload("Dataset").
			Preload("DefaultLocation").
			Preload("Locations").
			Where("dataset_id = ?", datasetID).
			Find(&objects).Error
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
		return tx.
			Preload("Project").
			Preload("Dataset").
			Preload("DefaultLocation").
			Preload("Locations").
			Where("project_id = ?", projectID).
			Find(&objects).Error
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
		return tx.
			Preload("Project").
			Preload("Dataset").
			Preload("DefaultLocation").
			Preload("Locations").
			Table("objects AS o").
			Joins("LEFT JOIN object_group_revision_data_objects AS ogrdo ON o.id = ogrdo.object_id").
			Joins("LEFT JOIN object_group_revision_meta_objects AS ogrmo ON o.id = ogrmo.object_id").
			Joins("LEFT JOIN object_group_revisions AS ogr ON ogrdo.object_group_revision_id = ogr.id OR ogrmo.object_group_revision_id = ogr.id").
			Where("ogr.object_group_id = ?", objectGroupID).
			Find(&objects).Error
	})

	if err != nil {
		log.Error(err.Error())
		return nil, err
	}

	return objects, nil
}

// Get all data objects of the specific ObjectGroup.
func (read *Read) GetAllObjectGroupDataObjects(objectGroupID uuid.UUID) ([]*models.Object, error) {
	var objects []*models.Object

	err := crdbgorm.ExecuteTx(context.Background(), read.DB, nil, func(tx *gorm.DB) error {
		return tx.
			Preload("Project").
			Preload("Dataset").
			Preload("DefaultLocation").
			Preload("Locations").
			Table("objects AS o").
			Joins("INNER JOIN object_group_revision_data_objects AS ogrdo ON o.id = ogrdo.object_id").
			Joins("INNER JOIN object_group_revisions AS ogr ON ogrdo.object_group_revision_id = ogr.id").
			Where("ogr.object_group_id = ?", objectGroupID).
			Find(&objects).Error
	})

	if err != nil {
		log.Error(err.Error())
		return nil, err
	}

	return objects, nil
}

// Get all meta objects of the specific ObjectGroup.
func (read *Read) GetAllObjectGroupMetaObjects(objectGroupID uuid.UUID) ([]*models.Object, error) {
	var objects []*models.Object

	err := crdbgorm.ExecuteTx(context.Background(), read.DB, nil, func(tx *gorm.DB) error {
		return tx.
			Preload("Project").
			Preload("Dataset").
			Preload("DefaultLocation").
			Preload("Locations").
			Table("objects AS o").
			Joins("INNER JOIN object_group_revision_meta_objects AS ogrmo ON o.id = ogrmo.object_id").
			Joins("INNER JOIN object_group_revisions AS ogr ON ogrmo.object_group_revision_id = ogr.id").
			Where("ogr.object_group_id = ?", objectGroupID).
			Find(&objects).Error
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
		return tx.
			Preload("Project").
			Preload("Dataset").
			Preload("DefaultLocation").
			Preload("Locations").
			Table("objects AS o").
			Joins("LEFT JOIN object_group_revision_data_objects AS ogrdo ON o.id = ogrdo.object_id").
			Joins("LEFT JOIN object_group_revision_meta_objects AS ogrmo ON o.id = ogrmo.object_id").
			Where("ogrdo.object_group_revision_id = ? OR ogrmo.object_group_revision_id = ?", revisionID, revisionID).
			Find(&objects).Error
	})

	if err != nil {
		log.Error(err.Error())
		return nil, err
	}

	return objects, nil
}

// Get all data objects of the specific ObjectGrouprevision.
func (read *Read) GetAllObjectGroupRevisionDataObjects(revisionID uuid.UUID) ([]*models.Object, error) {
	var objects []*models.Object

	err := crdbgorm.ExecuteTx(context.Background(), read.DB, nil, func(tx *gorm.DB) error {
		return tx.
			Preload("Project").
			Preload("Dataset").
			Preload("DefaultLocation").
			Preload("Locations").
			Table("objects AS o").
			Joins("Inner JOIN object_group_revision_data_objects AS ogrdo ON o.id = ogrdo.object_id").
			Where("ogrdo.object_group_revision_id = ?", revisionID, revisionID).
			Find(&objects).Error
	})

	if err != nil {
		log.Error(err.Error())
		return nil, err
	}

	return objects, nil
}

// Get all meta objects of the specific ObjectGrouprevision.
func (read *Read) GetAllObjectGroupRevisionMetaObjects(revisionID uuid.UUID) ([]*models.Object, error) {
	var objects []*models.Object

	err := crdbgorm.ExecuteTx(context.Background(), read.DB, nil, func(tx *gorm.DB) error {
		return tx.
			Preload("Project").
			Preload("Dataset").
			Preload("DefaultLocation").
			Preload("Locations").
			Table("objects AS o").
			Joins("INNER JOIN object_group_revision_meta_objects AS ogrmo ON o.id = ogrmo.object_id").
			Where("ogrmo.object_group_revision_id = ?", revisionID, revisionID).
			Find(&objects).Error
	})

	if err != nil {
		log.Error(err.Error())
		return nil, err
	}

	return objects, nil
}

func (read *Read) GetObjectGroupRevisionsInDateRange(datasetID uuid.UUID, startDate time.Time, endDate time.Time) ([]*models.ObjectGroupRevision, error) {
	var objectGroupRevisions []*models.ObjectGroupRevision

	err := crdbgorm.ExecuteTx(context.Background(), read.DB, nil, func(tx *gorm.DB) error {
		preloadConf := tx.
			Preload("Labels").
			Preload("Objects").
			Preload("Objects.Labels").
			Preload("Objects.Locations").
			Preload("Objects.DefaultLocation").
			Preload("MetaObjects")

		return preloadConf.
			Where("dataset_id = ? AND generated BETWEEN ? AND ?", datasetID, startDate, endDate).
			Find(&objectGroupRevisions).Error
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
		return tx.
			Preload("Project").
			Preload("Dataset").
			Preload("Labels").
			Preload("Locations").
			Preload("DefaultLocation").
			Find(&objects).Error
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
		err := tx.
			Preload("CurrentObjectGroupRevision").
			Preload("CurrentObjectGroupRevision.Labels").
			Preload("CurrentObjectGroupRevision.Objects").
			Preload("CurrentObjectGroupRevision.Objects.Locations").
			Preload("CurrentObjectGroupRevision.Objects.DefaultLocation").
			Preload("CurrentObjectGroupRevision.MetaObjects").
			Preload("CurrentObjectGroupRevision.MetaObjects.Locations").
			Preload("CurrentObjectGroupRevision.MetaObjects.DefaultLocation").
			Where("dataset_id = ?", datasetID).
			FindInBatches(&objectGroups, 10000, func(tx *gorm.DB, batch int) error {
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
		preloadConf := read.DB.
			Preload("CurrentObjectGroupRevision").
			Preload("CurrentObjectGroupRevision.Labels").
			Preload("CurrentObjectGroupRevision.Objects").
			Preload("CurrentObjectGroupRevision.Objects.Locations").
			Preload("CurrentObjectGroupRevision.Objects.DefaultLocation").
			Preload("CurrentObjectGroupRevision.MetaObjects").
			Preload("CurrentObjectGroupRevision.MetaObjects.Locations").
			Preload("CurrentObjectGroupRevision.MetaObjects.DefaultLocation")

		err := preloadConf.
			Where("dataset_id = ? AND created_at BETWEEN ? AND ?", datasetID, startDate, endDate).
			FindInBatches(&objectGroups, 10000, func(tx *gorm.DB, batch int) error {
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
		return tx.
			Preload("Labels").
			Preload("Objects").
			Preload("Objects.Locations").
			Preload("Objects.DefaultLocation").
			Preload("MetaObjects").
			Preload("MetaObjects.Locations").
			Preload("MetaObjects.DefaultLocation").
			Where("id IN ? and status NOT IN ?", objectGroupID, status).
			Find(&datasetVersions).Error
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
