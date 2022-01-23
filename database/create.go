package database

import (
	"context"
	"encoding/base64"
	"fmt"

	"github.com/cockroachdb/cockroach-go/v2/crdb/crdbgorm"
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

	err := crdbgorm.ExecuteTx(context.Background(), create.DB, nil, func(tx *gorm.DB) error {
		return tx.Create(&project).Error
	})
	if err != nil {
		log.Error(err.Error())
		return "", err
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

	projectID, err := uuid.Parse(request.ProjectId)
	if err != nil {
		log.Debug(err.Error())
		return "", err
	}

	dataset := models.Dataset{
		Name:        request.Name,
		Description: request.Description,
		Metadata:    metadataList,
		Labels:      labels,
		ProjectID:   projectID,
		IsPublic:    false,
	}

	err = crdbgorm.ExecuteTx(context.Background(), create.DB, nil, func(tx *gorm.DB) error {
		return tx.Create(&dataset).Error
	})

	if err != nil {
		log.Println(err.Error())
		return "", err
	}

	bucket, err := create.S3Handler.CreateBucket(dataset.ID)
	if err != nil {
		log.Println(err.Error())
		err = create.DB.Delete(&dataset).Error
		if err != nil {
			log.Error(err.Error())
			return "", err
		}
		return "", err
	}

	err = crdbgorm.ExecuteTx(context.Background(), create.DB, nil, func(tx *gorm.DB) error {
		dataset.Bucket = bucket
		return tx.Model(&models.Dataset{}).Where("ID = ?", dataset.ID).Update("Bucket", bucket).Error
	})
	if err != nil {
		log.Error(err.Error())
		return "", err
	}

	return dataset.ID.String(), nil
}

func (create *Create) CreateObjectGroup(request *services.CreateObjectGroupRequest, bucket string) (*models.ObjectGroup, error) {
	dataset := &models.Dataset{}

	datasetID, err := uuid.Parse(request.GetDatasetId())
	if err != nil {
		log.Debug(err.Error())
		return nil, err
	}

	dataset.ID = datasetID

	err = crdbgorm.ExecuteTx(context.Background(), create.DB, nil, func(tx *gorm.DB) error {
		return tx.Find(dataset).Error
	})

	if err != nil {
		log.Error(err.Error())
		return nil, fmt.Errorf("could not read datasetID")
	}

	objectGroupModel, objects, err := create.prepareObjectGroupForInsert(request, dataset, bucket)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	err = crdbgorm.ExecuteTx(context.Background(), create.DB, nil, func(tx *gorm.DB) error {
		objectGroupModel.Objects = objects

		if err := tx.Create(&objectGroupModel).Error; err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		log.Println(err.Error())
		return nil, fmt.Errorf("error while creating entries for object group")
	}

	return &objectGroupModel, nil
}

func (create *Create) CreateObjectGroupBatch(batchRequest *services.CreateObjectGroupBatchRequest, bucket string) ([]models.ObjectGroup, error) {
	var objectgroups []models.ObjectGroup
	var objectgroupsObjects [][]models.Object

	dataset := &models.Dataset{}

	datasetID, err := uuid.Parse(batchRequest.GetRequests()[0].GetDatasetId())
	if err != nil {
		log.Debug(err.Error())
		return nil, err
	}

	dataset.ID = datasetID
	result := create.DB.Find(dataset)
	if result.Error != nil {
		log.Println(result.Error.Error())
		return nil, result.Error
	}

	for _, request := range batchRequest.GetRequests() {
		objectGroup, objects, err := create.prepareObjectGroupForInsert(request, dataset, bucket)
		if err != nil {
			log.Println(err.Error())
			return nil, err
		}
		objectGroup.Objects = objects
		objectgroups = append(objectgroups, objectGroup)
		objectgroupsObjects = append(objectgroupsObjects, objects)
	}

	err = crdbgorm.ExecuteTx(context.Background(), create.DB, nil, func(tx *gorm.DB) error {
		if err = tx.Create(&objectgroups).Error; err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		log.Println(err.Error())
		return nil, fmt.Errorf("could not create error group")
	}

	return objectgroups, nil
}

func (create *Create) prepareObjectGroupForInsert(request *services.CreateObjectGroupRequest, dataset *models.Dataset, bucket string) (models.ObjectGroup, []models.Object, error) {
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
		location := create.S3Handler.CreateLocation(dataset.ProjectID, dataset.ID, uuid, protoObject.Filename, bucket)

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
	var err error
	for _, objectGroupID := range request.ObjectGroupIds {
		objectGroup := models.ObjectGroup{}

		objectGroup.ID, err = uuid.Parse(objectGroupID)
		if err != nil {
			log.Debug(err.Error())
			return uuid.UUID{}, err
		}

		objectGroups = append(objectGroups, objectGroup)
	}

	datasetID, err := uuid.Parse(request.DatasetId)
	if err != nil {
		log.Debug(err.Error())
		return uuid.UUID{}, err
	}

	version := &models.DatasetVersion{
		Name:            request.Name,
		Labels:          labels,
		Metadata:        metadataList,
		Description:     request.Description,
		DatasetID:       datasetID,
		MajorVersion:    uint(request.Version.Major),
		MinorVersion:    uint(request.Version.Minor),
		PatchVersion:    uint(request.Version.Patch),
		Stage:           request.Version.GetStage().String(),
		RevisionVersion: uint(request.GetVersion().Revision),
		ProjectID:       projectID,
		ObjectGroups:    objectGroups,
	}

	err = crdbgorm.ExecuteTx(context.Background(), create.DB, nil, func(tx *gorm.DB) error {
		if err := tx.Omit("ObjectGroups.*").Create(&version).Error; err != nil {
			log.Errorln(err.Error())
			return err
		}

		return nil
	})

	if err != nil {
		log.Errorf(err.Error())
		return uuid.UUID{}, fmt.Errorf("could not create dataset version database entry")
	}

	return version.ID, nil
}

func (create *Create) AddUserToProject(request *services.AddUserToProjectRequest) error {
	projectID, err := uuid.Parse(request.GetProjectId())
	if err != nil {
		log.Error(err.Error())
		return err
	}

	user := &models.User{
		UserOauth2ID: request.GetUserId(),
		ProjectID:    projectID,
	}

	err = crdbgorm.ExecuteTx(context.Background(), create.DB, nil, func(tx *gorm.DB) error {
		return tx.Create(user).Error
	})

	if err != nil {
		log.Error(err.Error())
		return fmt.Errorf("could not add user to project")
	}

	return nil
}

func (create *Create) CreateStreamGroup(projectID uuid.UUID, resourceType string, resourceID uuid.UUID, subject string, subResource bool) (*models.StreamGroup, error) {
	streamGroupEntry := &models.StreamGroup{
		ResourceID:     resourceID,
		ProjectID:      projectID,
		ResourceType:   resourceType,
		UseSubResource: subResource,
		Subject:        subject,
	}

	err := crdbgorm.ExecuteTx(context.Background(), create.DB, nil, func(tx *gorm.DB) error {
		return tx.Create(streamGroupEntry).Error
	})

	if err != nil {
		log.Error(err.Error())
		return nil, fmt.Errorf("could not create stream group")
	}

	return streamGroupEntry, nil
}

func (create *Create) CreateAPIToken(request *services.CreateAPITokenRequest, userOauth2ID string) (string, error) {
	rndBytes, err := util.GenerateRandomString(45)
	if err != nil {
		log.Println(err.Error())
		return "", err
	}

	base64String := base64.StdEncoding.EncodeToString(rndBytes)

	projectID, err := uuid.Parse(request.GetId())
	if err != nil {
		log.Debug(err.Error())
		return "", err
	}

	userUUID, err := uuid.Parse(userOauth2ID)
	if err != nil {
		log.Debug(err.Error())
		return "", err
	}

	apiToken := &models.APIToken{
		Token:     base64String,
		ProjectID: projectID,
		UserUUID:  userUUID,
	}

	err = crdbgorm.ExecuteTx(context.Background(), create.DB, nil, func(tx *gorm.DB) error {
		return tx.Create(apiToken).Error
	})

	if err != nil {
		log.Error(err.Error())
		return "", fmt.Errorf("could not create api token")
	}

	return base64String, nil
}
