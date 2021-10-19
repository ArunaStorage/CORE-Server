package database

import (
	"github.com/ScienceObjectsDB/CORE-Server/models"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

type Delete struct {
	*Common
}

func (handler *Delete) DeleteObjectGroup(objectGroupID uuid.UUID) error {
	objectGroup := &models.ObjectGroup{}
	objectGroup.ID = objectGroupID

	if err := handler.DB.Select("Labels", "Metadata", "Objects", "ObjectGroupRevisions", "ObjectGroupRevisions.Labels", "ObjectGroupRevisions.Metadata").Unscoped().Delete(objectGroup).Error; err != nil {
		log.Println(err.Error())
		return err
	}
	return nil
}

func (handler *Delete) DeleteDataset(datasetID uuid.UUID) error {
	dataset := &models.Dataset{}
	dataset.ID = datasetID

	if err := handler.DB.Select("Labels", "Metadata", "Objects", "ObjectGroups", "DatasetVersion", "ObjectGroups.ObjectGroupRevision", "ObjectsGroups.Objects").Unscoped().Delete(dataset).Error; err != nil {
		log.Println(err.Error())
		return err
	}

	return nil
}

func (handler *Delete) DeleteDatasetVersion(datasetVersionID uuid.UUID) error {
	version := &models.DatasetVersion{}
	version.ID = datasetVersionID

	if err := handler.DB.Select("Labels", "Metadata").Unscoped().Delete(version).Error; err != nil {
		log.Println(err.Error())
		return err
	}

	return nil
}

func (handler *Delete) DeleteProject(projectID uuid.UUID) error {
	project := &models.Project{}
	project.ID = projectID

	if err := handler.DB.Select("Labels", "Metadata", "User", "APIToken", "Datasets", "Datasets.ObjectGroupRevisions", "Datasets.Objects").Unscoped().Delete(project).Error; err != nil {
		log.Println(err.Error())
		return err
	}

	return nil
}

func (handler *Delete) DeleteAPIToken(tokenID uuid.UUID) error {
	token := &models.APIToken{}
	token.ID = tokenID

	if err := handler.DB.Delete(token).Error; err != nil {
		log.Println(err.Error())
		return err
	}

	return nil
}
