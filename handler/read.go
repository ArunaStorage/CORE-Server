package handler

import (
	log "github.com/sirupsen/logrus"

	"github.com/ScienceObjectsDB/CORE-Server/models"
)

type Read struct {
	*Common
}

func (read *Read) GetProject(projectID uint) (*models.Project, error) {
	project := &models.Project{}
	project.ID = projectID

	if err := read.DB.First(project).Error; err != nil {
		log.Println(err.Error())
		return nil, err
	}

	return project, nil
}

func (read *Read) GetDataset(datasetID uint) (*models.Dataset, error) {
	dataset := &models.Dataset{}
	dataset.ID = datasetID

	if err := read.DB.First(dataset).Error; err != nil {
		log.Println(err.Error())
		return nil, err
	}

	return dataset, nil
}

func (read *Read) GetObjectGroup(objectGroupID uint) (*models.ObjectGroup, error) {
	objectGroup := &models.ObjectGroup{}
	objectGroup.ID = objectGroupID

	if err := read.DB.Preload("Metadata").Preload("Labels").Preload("Objects").First(objectGroup).Error; err != nil {
		log.Println(err.Error())
		return nil, err
	}

	return objectGroup, nil
}

func (read *Read) GetObjectGroupRevision(objectGroupRevisionID uint) (*models.ObjectGroupRevision, error) {
	revision := &models.ObjectGroupRevision{}
	revision.ID = objectGroupRevisionID

	if err := read.DB.Preload("Metadata").Preload("Labels").First(revision).Error; err != nil {
		log.Println(err.Error())
		return nil, err
	}

	return revision, nil
}

func (read *Read) GetObjectGroupRevisionsObjects(objectGroupRevisionID uint) ([]*models.Object, error) {
	objects := make([]*models.Object, 0)

	if err := read.DB.Preload("Labels").Preload("Metadata").Where("object_group_revision_id = ?", objectGroupRevisionID).Find(&objects).Error; err != nil {
		log.Println(err.Error())
		return nil, err
	}

	return objects, nil
}

func (read *Read) GetProjectDatasets(projectID uint) ([]*models.Dataset, error) {
	objects := make([]*models.Dataset, 0)

	if err := read.DB.Preload("Labels").Preload("Metadata").Where("project_id = ?", projectID).Find(&objects).Error; err != nil {
		log.Println(err.Error())
		return nil, err
	}

	return objects, nil
}

func (read *Read) GetDatasetObjectGroups(datasetID uint) ([]*models.ObjectGroup, error) {
	objectGroups := make([]*models.ObjectGroup, 0)
	if err := read.DB.Preload("Labels").Preload("Metadata").Where("dataset_id = ?", datasetID).Find(&objectGroups).Error; err != nil {
		log.Println(err.Error())
		return nil, err
	}

	return objectGroups, nil
}

func (read *Read) GetObject(objectID uint) (*models.Object, error) {
	object := models.Object{}
	object.ID = objectID

	if err := read.DB.Preload("Location").First(&object).Error; err != nil {
		log.Println(err.Error())
		return nil, err
	}

	return &object, nil
}

func (read *Read) GetCurrentObjectGroupRevisions(dataset_id uint) ([]*models.ObjectGroupRevision, error) {
	var revisions []*models.ObjectGroupRevision

	if err := read.DB.Preload("Metadata").Preload("Labels").Raw(
		"SELECT * FROM ( SELECT object_group_id, MAX(revision) as revision FROM object_group_revisions WHERE dataset_id = ? GROUP BY object_group_id) t JOIN object_group_revisions m USING(object_group_id, revision)", dataset_id,
	).Scan(&revisions).Error; err != nil {
		log.Println(err.Error())
		return nil, err
	}

	return revisions, nil
}

func (read *Read) GetObjectGroupRevisions(objectGroupID uint) ([]*models.ObjectGroupRevision, error) {
	var revisions []*models.ObjectGroupRevision
	if err := read.DB.Preload("Metadata").Preload("Labels").Where("object_group_id = ?", objectGroupID).Find(&revisions).Error; err != nil {
		log.Println(err.Error())
		return nil, err
	}

	return revisions, nil
}

func (read *Read) GetDatasetVersion(versionID uint) (*models.DatasetVersion, error) {
	datasetVersion := &models.DatasetVersion{}
	datasetVersion.ID = versionID

	if err := read.DB.Preload("Labels").Preload("Metadata").Find(datasetVersion).Error; err != nil {
		log.Println(err.Error())
		return nil, err
	}

	return datasetVersion, nil
}

func (read *Read) GetDatasetVersions(datasetID uint) ([]models.DatasetVersion, error) {
	var datasetVersions []models.DatasetVersion
	if err := read.DB.Preload("Metadata").Preload("Labels").Where("dataset_id = ?", datasetID).Find(datasetVersions).Error; err != nil {
		log.Println(err.Error())
		return nil, err
	}

	return datasetVersions, nil
}

func (read *Read) GetAPIToken(userOAuth2ID string) ([]models.APIToken, error) {
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

func (read *Read) GetDatasetVersionWithRevisions(datasetVersionID uint) (*models.DatasetVersion, error) {
	version := &models.DatasetVersion{}
	version.ID = datasetVersionID

	if err := read.DB.Preload("object_group_revisions").First(version).Error; err != nil {
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

func (read *Read) GetCurrentObjectGroupRevision(objectGroupID uint) (*models.ObjectGroupRevision, error) {
	revision := &models.ObjectGroupRevision{}
	if err := read.DB.Preload("Objects").Raw("select * from object_group_revisions where revision =(select MAX(revision) from object_group_revisions) AND object_group_id = ?", objectGroupID).First(revision).Error; err != nil {
		return nil, err
	}

	return revision, nil
}

func (read *Read) GetAllDatasetObjects(datasetID uint) ([]*models.Object, error) {
	var objects []*models.Object
	if err := read.DB.Preload("Location").Where("dataset_id = ?", datasetID).Find(&objects).Error; err != nil {
		log.Println(err.Error())
		return nil, err
	}

	return objects, nil
}

func (read *Read) GetAllProjectObjects(projectID uint) ([]*models.Object, error) {
	var objects []*models.Object
	if err := read.DB.Preload("Location").Where("project_id = ?", projectID).Find(&objects).Error; err != nil {
		log.Println(err.Error())
		return nil, err
	}

	return objects, nil
}

func (read *Read) GetAllObjectGroupObjects(objectGroupID uint) ([]*models.Object, error) {
	var objects []*models.Object
	if err := read.DB.Preload("Location").Where("object_group_id = ?", objectGroupID).Find(&objects).Error; err != nil {
		log.Println(err.Error())
		return nil, err
	}

	return objects, nil
}

func (read *Read) GetAllObjectGroupRevisionObjects(revisionID uint) ([]*models.Object, error) {
	var objects []*models.Object
	if err := read.DB.Preload("Location").Where("object_group_revision_id = ?", revisionID).Find(&objects).Error; err != nil {
		log.Println(err.Error())
		return nil, err
	}

	return objects, nil
}
