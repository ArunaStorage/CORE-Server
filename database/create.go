package database

import (
	"encoding/base64"
	"fmt"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"

	"github.com/ScienceObjectsDB/CORE-Server/models"
	"github.com/ScienceObjectsDB/CORE-Server/util"
	services "github.com/ScienceObjectsDB/go-api/api/services/v1"
)

type Create struct {
	*Common
}

func (create *Create) CreateProject(request *services.CreateProjectRequest, userID string) (uint, error) {
	labels := []models.Label{}
	for _, protoLabel := range request.Labels {
		label := models.Label{}
		labels = append(labels, *label.FromProtoModel(protoLabel))
	}

	metadataList := []models.Metadata{}
	for _, protoMetadata := range request.Metadata {
		metadata := &models.Metadata{}
		metadataList = append(metadataList, *metadata.FromProtoModel(protoMetadata))
	}

	project := models.Project{
		Description: request.Description,
		Name:        request.Name,
		Users: []models.User{
			{
				UserOauth2ID: userID,
			},
		},
		Labels:   labels,
		Metadata: metadataList,
	}

	result := create.DB.Create(&project)
	if result.Error != nil {
		log.Println(result.Error.Error())
		return 0, result.Error
	}

	return project.ID, nil
}

func (create *Create) CreateDataset(request *services.CreateDatasetRequest) (uint, error) {
	labels := []models.Label{}
	for _, protoLabel := range request.Labels {
		label := models.Label{}
		labels = append(labels, *label.FromProtoModel(protoLabel))
	}

	metadataList := []models.Metadata{}
	for _, protoMetadata := range request.Metadata {
		metadata := &models.Metadata{}
		metadataList = append(metadataList, *metadata.FromProtoModel(protoMetadata))
	}

	dataset := models.Dataset{
		Name:        request.Name,
		Description: request.Description,
		Metadata:    metadataList,
		Labels:      labels,
		ProjectID:   uint(request.ProjectId),
		IsPublic:    false,
	}

	result := create.DB.Create(&dataset)
	if result.Error != nil {
		log.Println(result.Error.Error())
		return 0, result.Error
	}

	return dataset.ID, nil
}

func (create *Create) CreateObjectGroup(request *services.CreateObjectGroupRequest) (uint, error) {
	dataset := models.Dataset{}

	dataset.ID = uint(request.GetDatasetId())
	result := create.DB.Find(&dataset)
	if result.Error != nil {
		log.Println(result.Error.Error())
		return 0, result.Error
	}

	labels := []models.Label{}
	for _, protoLabel := range request.Labels {
		label := models.Label{}
		labels = append(labels, *label.FromProtoModel(protoLabel))
	}

	metadataList := []models.Metadata{}
	for _, protoMetadata := range request.Metadata {
		metadata := &models.Metadata{}
		metadataList = append(metadataList, *metadata.FromProtoModel(protoMetadata))
	}

	objectGroupModel := &models.ObjectGroup{
		DatasetID:   dataset.ID,
		ProjectID:   dataset.ProjectID,
		Name:        request.Name,
		Description: request.Description,
		Metadata:    metadataList,
		Labels:      labels,
		Generated:   request.Generated.AsTime(),
	}

	objects := make([]models.Object, 0)

	for _, protoObject := range request.GetObjects() {
		uuid := uuid.New().String()
		location := create.S3Handler.CreateLocation(dataset.ProjectID, dataset.ID, uuid, protoObject.Filename)

		labels := []models.Label{}
		for _, protoLabel := range protoObject.Labels {
			label := models.Label{}
			labels = append(labels, *label.FromProtoModel(protoLabel))
		}

		metadataList := []models.Metadata{}
		for _, protoMetadata := range protoObject.Metadata {
			metadata := &models.Metadata{}
			metadataList = append(metadataList, *metadata.FromProtoModel(protoMetadata))
		}

		object := models.Object{
			Filename:   protoObject.Filename,
			Filetype:   protoObject.Filetype,
			ContentLen: protoObject.ContentLen,
			Location:   location,
			Labels:     labels,
			Metadata:   metadataList,
			ObjectUUID: uuid,
			ProjectID:  dataset.ProjectID,
			DatasetID:  dataset.ID,
		}

		objects = append(objects, object)
	}

	create.DB.Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(objectGroupModel).Error; err != nil {
			log.Println(err.Error())
			return fmt.Errorf("could not create object group")
		}

		for _, object := range objects {
			object.ObjectGroupID = objectGroupModel.ID
		}

		objectGroupModel.Objects = objects

		if err := tx.Save(objectGroupModel).Error; err != nil {
			log.Println(err.Error())
			return fmt.Errorf("could not create object group")
		}

		return nil
	})

	return objectGroupModel.ID, nil
}

func (create *Create) CreateDatasetVersion(request *services.ReleaseDatasetVersionRequest, projectID uint) (uint, error) {
	labels := []models.Label{}
	for _, protoLabel := range request.Labels {
		label := models.Label{}
		labels = append(labels, *label.FromProtoModel(protoLabel))
	}

	metadataList := []models.Metadata{}
	for _, protoMetadata := range request.Metadata {
		metadata := &models.Metadata{}
		metadataList = append(metadataList, *metadata.FromProtoModel(protoMetadata))
	}

	objectGroups := make([]models.ObjectGroup, 0)
	for _, objectGroupID := range request.ObjectGroupIds {
		objectGroup := models.ObjectGroup{}
		objectGroup.ID = uint(objectGroupID)

		objectGroups = append(objectGroups, objectGroup)
	}

	version := &models.DatasetVersion{
		Name:            request.Name,
		Labels:          labels,
		Metadata:        metadataList,
		Description:     request.Description,
		DatasetID:       uint(request.DatasetId),
		MajorVersion:    uint(request.Version.Major),
		MinorVersion:    uint(request.Version.Minor),
		PatchVersion:    uint(request.Version.Patch),
		Stage:           request.Version.GetStage().String(),
		RevisionVersion: uint(request.GetVersion().Revision),
		ProjectID:       projectID,
		ObjectGroups:    objectGroups,
	}

	if err := create.DB.Create(&version).Error; err != nil {
		log.Println(err.Error())
		return 0, err
	}

	return version.ID, nil
}

func (create *Create) AddUserToProject(request *services.AddUserToProjectRequest) error {
	user := &models.User{
		UserOauth2ID: request.GetUserId(),
		ProjectID:    uint(request.GetProjectId()),
	}

	if err := create.DB.Create(user).Error; err != nil {
		log.Println(err.Error())
		return err
	}

	return nil
}

func (create *Create) CreateAPIToken(request *services.CreateAPITokenRequest, userOauth2ID string) (string, error) {
	rndBytes, err := util.GenerateRandomString(45)
	if err != nil {
		log.Println(err.Error())
		return "", err
	}

	base64String := base64.StdEncoding.EncodeToString(rndBytes)

	apiToken := &models.APIToken{
		Token:     base64String,
		ProjectID: uint(request.GetId()),
		UserUUID:  userOauth2ID,
	}

	if err := create.DB.Create(apiToken).Error; err != nil {
		return "", err
	}

	return base64String, nil
}
