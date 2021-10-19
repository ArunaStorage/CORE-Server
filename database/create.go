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

// Handles Create operations
type Create struct {
	*Common
}

func (create *Create) CreateProject(request *services.CreateProjectRequest, userID string) (string, error) {
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
		return "", result.Error
	}

	return project.ID.String(), nil
}

func (create *Create) CreateDataset(request *services.CreateDatasetRequest) (string, error) {
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
		ProjectID:   uuid.MustParse(request.GetProjectId()),
		IsPublic:    false,
	}

	result := create.DB.Create(&dataset)
	if result.Error != nil {
		log.Println(result.Error.Error())
		return "", result.Error
	}

	return dataset.ID.String(), nil
}

func (create *Create) CreateObjectGroup(request *services.CreateObjectGroupRequest) (*models.ObjectGroup, error) {
	dataset := &models.Dataset{}

	dataset.ID = uuid.MustParse(request.GetDatasetId())
	result := create.DB.Find(dataset)
	if result.Error != nil {
		log.Println(result.Error.Error())
		return nil, result.Error
	}

	objectGroupModel, objects, err := create.prepareObjectGroupForInsert(request, dataset)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	create.DB.Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&objectGroupModel).Error; err != nil {
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

	return &objectGroupModel, nil
}

func (create *Create) CreateObjectGroupBatch(batchRequest *services.CreateObjectGroupBatchRequest) ([]models.ObjectGroup, error) {
	var objectgroups []models.ObjectGroup
	var objectgroupsObjects [][]models.Object

	dataset := &models.Dataset{}

	dataset.ID = uuid.MustParse(batchRequest.GetRequests()[0].GetDatasetId())
	result := create.DB.Find(dataset)
	if result.Error != nil {
		log.Println(result.Error.Error())
		return nil, result.Error
	}

	for _, request := range batchRequest.GetRequests() {
		objectGroup, objects, err := create.prepareObjectGroupForInsert(request, dataset)
		if err != nil {
			log.Println(err.Error())
			return nil, err
		}
		objectgroups = append(objectgroups, objectGroup)
		objectgroupsObjects = append(objectgroupsObjects, objects)
	}

	err := create.DB.Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&objectgroups).Error; err != nil {
			log.Println(err.Error())
			return fmt.Errorf("could not create object group")
		}

		for i, objectgroup := range objectgroups {
			objects := objectgroupsObjects[i]
			for _, object := range objects {
				object.ObjectGroupID = objectgroup.ID
			}

			objectgroups[i].Objects = objects
		}

		if err := tx.Save(&objectgroups).Error; err != nil {
			log.Println(err.Error())
			return fmt.Errorf("could not create object group")
		}

		return nil
	})

	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	return objectgroups, nil
}

func (create *Create) prepareObjectGroupForInsert(request *services.CreateObjectGroupRequest, dataset *models.Dataset) (models.ObjectGroup, []models.Object, error) {
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

	objectGroupModel := models.ObjectGroup{
		DatasetID:   dataset.ID,
		ProjectID:   dataset.ProjectID,
		Name:        request.Name,
		Description: request.Description,
		Metadata:    metadataList,
		Labels:      labels,
		Generated:   request.Generated.AsTime(),
	}

	objects := make([]models.Object, 0)

	for i, protoObject := range request.GetObjects() {
		uuid := uuid.New()
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
			Index:      uint64(i),
		}

		objects = append(objects, object)
	}

	return objectGroupModel, objects, nil
}

func (create *Create) CreateDatasetVersion(request *services.ReleaseDatasetVersionRequest, projectID uuid.UUID) (uuid.UUID, error) {
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
		objectGroup.ID = uuid.MustParse(objectGroupID)

		objectGroups = append(objectGroups, objectGroup)
	}

	version := &models.DatasetVersion{
		Name:            request.Name,
		Labels:          labels,
		Metadata:        metadataList,
		Description:     request.Description,
		DatasetID:       uuid.MustParse(request.DatasetId),
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
		return uuid.UUID{}, err
	}

	return version.ID, nil
}

func (create *Create) AddUserToProject(request *services.AddUserToProjectRequest) error {
	user := &models.User{
		UserOauth2ID: request.GetUserId(),
		ProjectID:    uuid.MustParse(request.GetProjectId()),
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
		ProjectID: uuid.MustParse(request.GetId()),
		UserUUID:  uuid.MustParse(userOauth2ID),
	}

	if err := create.DB.Create(apiToken).Error; err != nil {
		return "", err
	}

	return base64String, nil
}
