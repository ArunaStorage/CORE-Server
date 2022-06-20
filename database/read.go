package database

import (
	"context"
	"errors"
	"time"

	"github.com/ScienceObjectsDB/CORE-Server/models"
	"github.com/cockroachdb/cockroach-go/v2/crdb/crdbgorm"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gorm.io/gorm"

	v1storagemodels "github.com/ScienceObjectsDB/go-api/sciobjsdb/api/storage/models/v1"
	v1servicemodels "github.com/ScienceObjectsDB/go-api/sciobjsdb/api/storage/services/v1"
	log "github.com/sirupsen/logrus"
)

type ID struct {
	ID uuid.UUID
}

type Read struct {
	*Common
}

// Get the specific Project.
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

// Get the specific Dataset.
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

// Get the specific ObjectGroupRevision.
func (read *Read) GetObjectGroupRevision(objectGroupRevisionsID uuid.UUID) (*models.ObjectGroupRevision, error) {
	objectGroupRevision := &models.ObjectGroupRevision{}
	objectGroupRevision.ID = objectGroupRevisionsID

	err := crdbgorm.ExecuteTx(context.Background(), read.DB, nil, func(tx *gorm.DB) error {
		return tx.
			Preload("Project").
			Preload("Dataset").
			Preload("Labels").
			// DatasetVersions should be fetched on demand
			Preload("DataObjects").
			Preload("MetaObjects").
			First(objectGroupRevision).Error
	})

	if err != nil {
		log.Errorln(err.Error())
		return nil, err
	}

	return objectGroupRevision, nil
}

// Get the specific ObjectGroup.
func (read *Read) GetObjectGroup(objectGroupID uuid.UUID) (*models.ObjectGroup, error) {
	objectGroup := &models.ObjectGroup{}
	objectGroup.ID = objectGroupID

	err := crdbgorm.ExecuteTx(context.Background(), read.DB, nil, func(tx *gorm.DB) error {
		return tx.Transaction(func(tx *gorm.DB) error {
			if err := tx.Preload("Dataset").Preload("Project").First(objectGroup).Error; err != nil {
				log.Errorln(err.Error())
				return err
			}

			objectGroupRevision := &models.ObjectGroupRevision{}
			objectGroupRevision.ID = objectGroup.CurrentObjectGroupRevisionID

			preloads := tx.Preload("Dataset").Preload("Project").Preload("MetaObjects.Locations").Preload("MetaObjects.DefaultLocation").Preload("DataObjects.Locations").Preload("DataObjects.DefaultLocation").Preload("DataObjects").Preload("MetaObjects").Preload("Labels").Preload("DataObjects.Labels")
			if err := preloads.First(objectGroupRevision).Error; err != nil {
				log.Errorln(err.Error())
				return err
			}

			objectGroup.CurrentObjectGroupRevision = *objectGroupRevision

			return nil
		})
	})

	if err != nil {
		log.Errorln(err.Error())
		return nil, err
	}

	return objectGroup, nil
}

// Get all datasets of the specific Project.
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

	return datasets, nil
}

func (read *Read) GetDatasetObjects(request *v1servicemodels.GetDatasetObjectsRequest) ([]*models.Object, error) {
	var objects []*models.Object
	var err error

	if request.LabelFilter == nil || len(request.LabelFilter.Labels) == 0 {
		objects, err = read.getDatasetObjects(request)
		if err != nil {
			log.Errorln(err.Error())
			return nil, err
		}
	} else {
		objects, err = read.getDatasetObjectsWithLabelFilter(request)
		if err != nil {
			log.Errorln(err.Error())
			return nil, err
		}
	}

	return objects, nil
}

func (read *Read) getDatasetObjects(request *v1servicemodels.GetDatasetObjectsRequest) ([]*models.Object, error) {
	objects := make([]*models.Object, 0)

	datasetUUID, err := uuid.Parse(request.GetId())
	if err != nil {
		log.Debugln(err)
		return nil, status.Error(codes.InvalidArgument, "could not parse dataset id")
	}

	preload := read.DB.
		Preload("Labels").
		Preload("Locations").
		Preload("DefaultLocation")

	crdbgorm.ExecuteTx(context.Background(), read.DB, nil, func(tx *gorm.DB) error {
		if request.PageRequest == nil || request.PageRequest.PageSize == 0 {
			return preload.Where("dataset_id = ?", datasetUUID).Find(&objects).Error

		} else if request.PageRequest != nil && request.PageRequest.LastUuid == "" {
			return preload.
				Where("dataset_id = ?", datasetUUID).
				Order("id asc").
				Limit(int(request.PageRequest.PageSize)).
				Find(&objects).Error

		} else if request.PageRequest != nil && request.PageRequest.LastUuid != "" && request.PageRequest.PageSize > 0 {
			return preload.
				Where("dataset_id = ? AND id > ?", datasetUUID, request.PageRequest.LastUuid).
				Order("id asc").
				Limit(int(request.PageRequest.PageSize)).
				Find(&objects).Error

		} else {
			log.Info("could not parse request")
			return errors.New("could not parse request")
		}
	})

	return objects, nil
}

func (read *Read) getDatasetObjectsWithLabelFilter(request *v1servicemodels.GetDatasetObjectsRequest) ([]*models.Object, error) {
	objects := make([]*models.Object, 0)

	datasetUUID, err := uuid.Parse(request.GetId())
	if err != nil {
		log.Debugln(err)
		return nil, status.Error(codes.InvalidArgument, "could not parse dataset id")
	}

	var labels [][]interface{}

	for _, requestLabel := range request.LabelFilter.Labels {
		labels = append(labels, []interface{}{requestLabel.Key, requestLabel.Value})
	}

	if len(labels) == 0 {
		labels = nil
	}

	var objectIDs []ID

	crdbgorm.ExecuteTx(context.Background(), read.DB, nil, func(tx *gorm.DB) error {

		if request.PageRequest == nil || request.PageRequest.PageSize == 0 {
			if err := tx.Model(&models.Object{}).
				Select("objects.id").
				Joins("inner join object_labels on objects.id = object_labels.object_id").
				Joins("inner join labels on object_labels.label_id = labels.id").
				Where("dataset_id = ? AND (key, value) in (?)", datasetUUID, labels).
				Group("objects.id").Having("COUNT(objects.id) = ?", len(request.LabelFilter.Labels)).
				Find(&objectIDs).Error; err != nil {
				log.Errorln(err.Error())
				return err
			}

			ids := make([]uuid.UUID, len(objectIDs))
			for i, id := range objectIDs {
				ids[i] = id.ID
			}

			if err := tx.Model(&models.Object{}).
				Preload("Labels").
				Preload("Locations").
				Preload("DefaultLocation").
				Where("id in ?", ids).
				Find(&objects).Error; err != nil {
				log.Errorln(err.Error())
				return err
			}

		} else if request.PageRequest != nil && request.PageRequest.LastUuid == "" {
			if err := tx.Model(&models.Object{}).
				Select("objects.id").
				Joins("inner join object_labels on objects.id = object_labels.object_id").
				Joins("inner join labels on object_labels.label_id = labels.id").
				Where("dataset_id = ? AND (key, value) in (?)", datasetUUID, labels).
				Group("objects.id").Having("COUNT(objects.id) = ?", len(request.LabelFilter.Labels)).
				Order("id asc").
				Limit(int(request.PageRequest.PageSize)).
				Find(&objectIDs).Error; err != nil {
				log.Errorln(err.Error())
				return err
			}

			ids := make([]uuid.UUID, len(objectIDs))
			for i, id := range objectIDs {
				ids[i] = id.ID
			}

			if err := tx.Model(&models.Object{}).
				Preload("Labels").
				Preload("Locations").
				Preload("DefaultLocation").
				Where("id in ?", ids).
				Find(objects).Error; err != nil {
				log.Errorln(err.Error())
				return err
			}

		} else if request.PageRequest != nil && request.PageRequest.LastUuid != "" && request.PageRequest.PageSize > 0 {
			if err := tx.Model(&models.Object{}).
				Select("objects.id").
				Joins("inner join object_labels on objects.id = object_labels.object_id").
				Joins("inner join labels on object_labels.label_id = labels.id").
				Where("dataset_id = ? AND (key, value) in (?) AND objects.id > ?", datasetUUID, labels, request.PageRequest.LastUuid).
				Group("objects.id").Having("COUNT(objects.id) = ?", len(request.LabelFilter.Labels)).
				Order("id asc").
				Limit(int(request.PageRequest.PageSize)).
				Find(&objectIDs).Error; err != nil {
				log.Errorln(err.Error())
				return err
			}

			ids := make([]uuid.UUID, len(objectIDs))
			for i, id := range objectIDs {
				ids[i] = id.ID
			}

			if err := tx.Model(&models.Object{}).
				Preload("Labels").
				Preload("Locations").
				Preload("DefaultLocation").
				Where("id in ?", ids).
				Find(objects).Error; err != nil {
				log.Errorln(err.Error())
				return err
			}

		} else {
			log.Info("could not parse request")
			return errors.New("could not parse request")
		}

		return nil
	})

	return objects, nil
}

// Get all object groups of the specific Dataset.
func (read *Read) GetDatasetObjectGroups(datasetID uuid.UUID, page *v1storagemodels.PageRequest) ([]*models.ObjectGroup, error) {
	objectGroups := make([]*models.ObjectGroup, 0)

	err := crdbgorm.ExecuteTx(context.Background(), read.DB, nil, func(tx *gorm.DB) error {
		preload := tx.
			Preload("Project").
			Preload("Dataset").
			Preload("CurrentObjectGroupRevision").
			Preload("CurrentObjectGroupRevision.Labels").
			Preload("CurrentObjectGroupRevision.DataObjects").
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

// Get the specific Object.
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

// Get the specific DatasetVersion.
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

// Get all dataset versions of the specific Dataset.
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

// Get all API tokens registered for the user with the specific OAuth2ID.
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

// Get the specific DatasetVersion including the full ObjectGroupRevisions.
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
			Preload("DataObjects").
			Preload("DataObjects.Labels").
			Preload("DataObjects.Locations").
			Preload("DataObjects.DefaultLocation").
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

//Get all projects the User is assigned to.
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

// Get all users assigned to the specific Project.
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

//Get all objects registered under the specific Dataset.
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

//Get all objects registered under the specific Project.
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

// Get all objects of the specific ObjectGroup.
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
// The Objects are ordered ascending by their index value.
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
			Order("o.index asc").
			Find(&objects).Error
	})

	if err != nil {
		log.Error(err.Error())
		return nil, err
	}

	return objects, nil
}

// Get all meta objects of the specific ObjectGroup.
// The Objects are ordered ascending by their index value.
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
			Order("o.index asc").
			Find(&objects).Error
	})

	if err != nil {
		log.Error(err.Error())
		return nil, err
	}

	return objects, nil
}

// Get all objects of the specific ObjectGrouprevision.
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
// The Objects are ordered ascending by their index value.
func (read *Read) GetAllObjectGroupRevisionDataObjects(revisionID uuid.UUID) ([]*models.Object, error) {
	var objects []*models.Object

	err := crdbgorm.ExecuteTx(context.Background(), read.DB, nil, func(tx *gorm.DB) error {
		return tx.
			Preload("Project").
			Preload("Dataset").
			Preload("DefaultLocation").
			Preload("Locations").
			Table("objects AS o").
			Joins("INNER JOIN object_group_revision_data_objects AS ogrdo ON o.id = ogrdo.object_id").
			Where("ogrdo.object_group_revision_id = ?", revisionID).
			Order("o.index asc").
			Find(&objects).Error
	})

	if err != nil {
		log.Error(err.Error())
		return nil, err
	}

	return objects, nil
}

// Get all meta objects of the specific ObjectGrouprevision.
// The Objects are ordered ascending by their index value.
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
			Where("ogrmo.object_group_revision_id = ?", revisionID).
			Order("o.index asc").
			Find(&objects).Error
	})

	if err != nil {
		log.Error(err.Error())
		return nil, err
	}

	return objects, nil
}

// Get all object group revisions of the specific Dataset which were generated between
// the provided start and end date. The start and end date is inclusive.
func (read *Read) GetObjectGroupRevisionsInDateRange(datasetID uuid.UUID, startDate time.Time, endDate time.Time) ([]*models.ObjectGroupRevision, error) {
	var objectGroupRevisions []*models.ObjectGroupRevision

	err := crdbgorm.ExecuteTx(context.Background(), read.DB, nil, func(tx *gorm.DB) error {
		preloadConf := tx.
			Preload("Labels").
			Preload("DataObjects").
			Preload("DataObjects.Labels").
			Preload("DataObjects.Locations").
			Preload("DataObjects.DefaultLocation").
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

// Get multiple Objects specified by the provided IDs.
func (read *Read) GetObjectsBatch(objectIds []uuid.UUID) ([]*models.Object, error) {
	objects := make([]*models.Object, len(objectIds))

	for i, id := range objectIds {
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

// Get all ObjectGroups of the specific Dataset.
// The ObjectGroups are send in batches of 10000 through the provided channel.
func (read *Read) GetDatasetObjectGroupsBatches(datasetID uuid.UUID, objectGroupsChan chan []*models.ObjectGroup) error {
	objectGroups := make([]*models.ObjectGroup, 0)

	err := crdbgorm.ExecuteTx(context.Background(), read.DB, nil, func(tx *gorm.DB) error {
		err := tx.
			Preload("CurrentObjectGroupRevision").
			Preload("CurrentObjectGroupRevision.Labels").
			Preload("CurrentObjectGroupRevision.DataObjects").
			Preload("CurrentObjectGroupRevision.DataObjects.Locations").
			Preload("CurrentObjectGroupRevision.DataObjects.DefaultLocation").
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

// Get all ObjectGroups of the specific Dataset in the specified datetime range. The start and end date is inclusive.
//
// The ObjectGroups are send in batches of 10000 through the provided channel.
func (read *Read) GetObjectGroupsInDateRangeBatches(datasetID uuid.UUID, startDate time.Time, endDate time.Time, objectGroupsChan chan []*models.ObjectGroup) error {
	var objectGroups []*models.ObjectGroup

	err := crdbgorm.ExecuteTx(context.Background(), read.DB, nil, func(tx *gorm.DB) error {
		preloadConf := read.DB.
			Preload("CurrentObjectGroupRevision").
			Preload("CurrentObjectGroupRevision.Labels").
			Preload("CurrentObjectGroupRevision.DataObjects").
			Preload("CurrentObjectGroupRevision.DataObjects.Locations").
			Preload("CurrentObjectGroupRevision.DataObjects.DefaultLocation").
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

// Get all ObjectGroupRevisions of the specific ObjectGroup in the specified datetime range. The start and end date is inclusive.
//
// The ObjectGroups are send in batches of 10000 through the provided channel.
func (read *Read) GetObjectGroupRevisionsByStatus(objectGroupID []string, status []string) ([]models.ObjectGroupRevision, error) {
	var datasetVersions []models.ObjectGroupRevision

	err := crdbgorm.ExecuteTx(context.Background(), read.DB, nil, func(tx *gorm.DB) error {
		return tx.
			Preload("Labels").
			Preload("DataObjects").
			Preload("DataObjects.Locations").
			Preload("DataObjects.DefaultLocation").
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

// Get the specific StreamGroup.
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
