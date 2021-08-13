package database

import (
	"github.com/ScienceObjectsDB/CORE-Server/models"
	log "github.com/sirupsen/logrus"
)

type Delete struct {
	*Common
}

func (handler *Delete) DeleteObjectGroupRevision(revisionID uint) error {
	revision := &models.ObjectGroupRevision{}
	revision.ID = revisionID

	if err := handler.DB.Select("Labels", "Metadata", "Objects", "Objects.Labels", "Objects.Metadata").Unscoped().Delete(revision).Error; err != nil {
		log.Println(err.Error())
		return err
	}

	return nil
}

func (handler *Delete) DeleteObjectGroup(objectGroupID uint) error {
	objectGroup := &models.ObjectGroup{}
	objectGroup.ID = objectGroupID

	if err := handler.DB.Select("Labels", "Metadata", "Objects", "ObjectGroupRevisions", "ObjectGroupRevisions.Labels", "ObjectGroupRevisions.Metadata").Unscoped().Delete(objectGroup).Error; err != nil {
		log.Println(err.Error())
		return err
	}
	return nil
}

func (handler *Delete) DeleteDataset(datasetID uint) error {
	dataset := &models.Dataset{}
	dataset.ID = datasetID

	if err := handler.DB.Select("Labels", "Metadata", "Objects", "ObjectGroups", "DatasetVersion", "ObjectGroups.ObjectGroupRevision", "ObjectsGroups.Objects").Unscoped().Delete(dataset).Error; err != nil {
		log.Println(err.Error())
		return err
	}

	return nil
}

func (handler *Delete) DeleteDatasetVersion(datasetVersionID uint) error {
	version := &models.DatasetVersion{}
	version.ID = datasetVersionID

	if err := handler.DB.Select("Labels", "Metadata").Unscoped().Delete(version).Error; err != nil {
		log.Println(err.Error())
		return err
	}

	return nil
}

func (handler *Delete) DeleteProject(projectID uint) error {
	project := &models.Project{}
	project.ID = projectID

	if err := handler.DB.Select("Labels", "Metadata", "User", "APIToken", "Datasets", "Datasets.ObjectGroupRevisions", "Datasets.Objects").Unscoped().Delete(project).Error; err != nil {
		log.Println(err.Error())
		return err
	}

	return nil
}
