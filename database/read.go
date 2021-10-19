package database

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"

	"github.com/ScienceObjectsDB/CORE-Server/models"
)

type Read struct {
	*Common
}

func (read *Read) GetProject(projectID uuid.UUID) (*models.Project, error) {
	project := &models.Project{}
	project.ID = projectID

	if err := read.DB.Preload("Labels").Preload("Metadata").First(project).Error; err != nil {
		log.Println(err.Error())
		return nil, err
	}

	return project, nil
}

func (read *Read) GetDataset(datasetID uuid.UUID) (*models.Dataset, error) {
	dataset := &models.Dataset{}
	dataset.ID = datasetID

	if err := read.DB.Preload("Labels").Preload("Metadata").First(dataset).Error; err != nil {
		log.Println(err.Error())
		return nil, err
	}

	return dataset, nil
}

func (read *Read) GetObjectGroup(objectGroupID uuid.UUID) (*models.ObjectGroup, error) {
	objectGroup := &models.ObjectGroup{}
	objectGroup.ID = objectGroupID

	if err := read.DB.Preload("Metadata").Preload("Labels").Preload("Objects").First(objectGroup).Error; err != nil {
		log.Println(err.Error())
		return nil, err
	}

	return objectGroup, nil
}

func (read *Read) GetObjectGroupRevisionsObjects(objectGroupRevisionID uuid.UUID) ([]*models.Object, error) {
	objects := make([]*models.Object, 0)

	if err := read.DB.Preload("Labels").Preload("Metadata").Where("object_group_revision_id = ?", objectGroupRevisionID).Find(&objects).Error; err != nil {
		log.Println(err.Error())
		return nil, err
	}

	return objects, nil
}

func (read *Read) GetProjectDatasets(projectID uuid.UUID) ([]*models.Dataset, error) {
	objects := make([]*models.Dataset, 0)

	if err := read.DB.Preload("Labels").Preload("Metadata").Where("project_id = ?", projectID).Find(&objects).Error; err != nil {
		log.Println(err.Error())
		return nil, err
	}

	return objects, nil
}

func (read *Read) GetDatasetObjectGroups(datasetID uuid.UUID) ([]*models.ObjectGroup, error) {
	objectGroups := make([]*models.ObjectGroup, 0)
	if err := read.DB.Preload("Objects.Location").Preload("Objects").Preload("Labels").Preload("Metadata").Where("dataset_id = ?", datasetID).Find(&objectGroups).Error; err != nil {
		log.Println(err.Error())
		return nil, err
	}

	return objectGroups, nil
}

func (read *Read) GetObject(objectID uuid.UUID) (*models.Object, error) {
	object := models.Object{}
	object.ID = objectID

	if err := read.DB.Preload("Labels").Preload("Metadata").Preload("Location").First(&object).Error; err != nil {
		log.Println(err.Error())
		return nil, err
	}

	return &object, nil
}

func (read *Read) GetDatasetVersion(versionID uuid.UUID) (*models.DatasetVersion, error) {
	datasetVersion := &models.DatasetVersion{}
	datasetVersion.ID = versionID

	if err := read.DB.Preload("Labels").Preload("Metadata").Preload("ObjectGroupRevisions").Find(datasetVersion).Error; err != nil {
		log.Println(err.Error())
		return nil, err
	}

	return datasetVersion, nil
}

func (read *Read) GetDatasetVersions(datasetID uuid.UUID) ([]models.DatasetVersion, error) {
	var datasetVersions []models.DatasetVersion
	if err := read.DB.Preload("Metadata").Preload("Labels").Where("dataset_id = ?", datasetID).Find(&datasetVersions).Error; err != nil {
		log.Println(err.Error())
		return nil, err
	}

	return datasetVersions, nil
}

func (read *Read) GetAPIToken(userOAuth2ID uuid.UUID) ([]models.APIToken, error) {
	user := &models.User{}

	if err := read.DB.Where("user_oauth2_id = ?", userOAuth2ID).Find(user).Error; err != nil {
		log.Println(err.Error())
		return nil, err
	}

	token := make([]models.APIToken, 0)
	if err := read.DB.Where("user_uuid = ?", userOAuth2ID).Find(&token).Error; err != nil {
		log.Println(err.Error())
		return nil, err
	}

	return token, nil
}

func (read *Read) GetDatasetVersionWithObjectGroups(datasetVersionID uuid.UUID) (*models.DatasetVersion, error) {
	version := &models.DatasetVersion{}
	version.ID = datasetVersionID

	if err := read.DB.Preload("ObjectGroups").First(version).Error; err != nil {
		log.Println(err.Error())
		return nil, err
	}

	return version, nil
}

func (read *Read) GetUserProjects(userIDOauth2 string) ([]*models.Project, error) {
	var users []*models.User
	if err := read.DB.Preload("Project").Where("user_oauth2_id = ?", userIDOauth2).Find(&users).Error; err != nil {
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
	if err := read.DB.Preload("Location").Where("dataset_id = ?", datasetID).Find(&objects).Error; err != nil {
		log.Println(err.Error())
		return nil, err
	}

	return objects, nil
}

func (read *Read) GetAllProjectObjects(projectID uuid.UUID) ([]*models.Object, error) {
	var objects []*models.Object
	if err := read.DB.Preload("Location").Where("project_id = ?", projectID).Find(&objects).Error; err != nil {
		log.Println(err.Error())
		return nil, err
	}

	return objects, nil
}

func (read *Read) GetAllObjectGroupObjects(objectGroupID uuid.UUID) ([]*models.Object, error) {
	var objects []*models.Object
	if err := read.DB.Preload("Location").Where("object_group_id = ?", objectGroupID).Find(&objects).Error; err != nil {
		log.Println(err.Error())
		return nil, err
	}

	return objects, nil
}

func (read *Read) GetAllObjectGroupRevisionObjects(revisionID uuid.UUID) ([]*models.Object, error) {
	var objects []*models.Object
	if err := read.DB.Preload("Location").Where("object_group_revision_id = ?", revisionID).Find(&objects).Error; err != nil {
		log.Println(err.Error())
		return nil, err
	}

	return objects, nil
}

func (read *Read) GetObjectGroupsInDateRange(datasetID uuid.UUID, startDate time.Time, endDate time.Time) ([]*models.ObjectGroup, error) {
	var objectGroups []*models.ObjectGroup
	preloadConf := read.DB.Preload("Metadata").Preload("Labels").Preload("Objects").Preload("Objects.Location").Preload("Objects.Metadata").Preload("Objects.Labels")
	if err := preloadConf.Where("dataset_id = ? AND generated  BETWEEN ? AND ?", datasetID, startDate, endDate).Find(&objectGroups).Error; err != nil {
		log.Println(err.Error())
		return nil, fmt.Errorf("could not read given date range")
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

	if err := read.DB.Preload("Metadata").Preload("Labels").Find(&objects).Error; err != nil {
		log.Println(err.Error())
		return nil, err
	}

	return objects, nil
}

// BatchedReads

func (read *Read) GetDatasetObjectGroupsBatches(datasetID uuid.UUID, objectGroupsChan chan []*models.ObjectGroup) error {
	objectGroups := make([]*models.ObjectGroup, 0)
	if err := read.DB.Preload("Objects.Location").Preload("Objects").Preload("Labels").Preload("Metadata").Where("dataset_id = ?", datasetID).FindInBatches(&objectGroups, 10000, func(tx *gorm.DB, batch int) error {
		var objectGroupsBatch []*models.ObjectGroup
		objectGroupsBatch = append(objectGroupsBatch, objectGroups...)

		objectGroupsChan <- objectGroupsBatch
		return nil
	}).Error; err != nil {
		log.Println(err.Error())
		return err
	}

	return nil
}

func (read *Read) GetObjectGroupsInDateRangeBatches(datasetID uuid.UUID, startDate time.Time, endDate time.Time, objectGroupsChan chan []*models.ObjectGroup) error {
	var objectGroups []*models.ObjectGroup
	preloadConf := read.DB.Preload("Metadata").Preload("Labels").Preload("Objects").Preload("Objects.Location").Preload("Objects.Metadata").Preload("Objects.Labels")
	if err := preloadConf.Where("dataset_id = ? AND generated  BETWEEN ? AND ?", datasetID, startDate, endDate).FindInBatches(&objectGroups, 10000, func(tx *gorm.DB, batch int) error {
		var objectGroupsBatch []*models.ObjectGroup
		objectGroupsBatch = append(objectGroupsBatch, objectGroups...)

		objectGroupsChan <- objectGroupsBatch
		return nil
	}).Error; err != nil {
		log.Println(err.Error())
		return fmt.Errorf("could not read given date range")
	}

	return nil
}
