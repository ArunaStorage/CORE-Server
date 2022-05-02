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
	v1storagemodels "github.com/ScienceObjectsDB/go-api/sciobjsdb/api/storage/models/v1"
	v1storageservices "github.com/ScienceObjectsDB/go-api/sciobjsdb/api/storage/services/v1"
)

// Handles Create operations
type Create struct {
	*Common
}

func (create *Create) CreateProject(request *v1storageservices.CreateProjectRequest, userID string) (string, error) {
	labels := []models.Label{}
	for _, protoLabel := range request.Labels {
		label := models.Label{}
		labels = append(labels, *label.FromProtoModel(protoLabel))
	}

	project := models.Project{
		Description: request.Description,
		Name:        request.Name,
		Users: []models.User{
			{
				UserOauth2ID: userID,
			},
		},
		Labels: labels,
		Status: v1storagemodels.Status_STATUS_AVAILABLE.String(),
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

func (create *Create) CreateDataset(request *v1storageservices.CreateDatasetRequest) (string, error) {
	datasetID := uuid.New()

	labels := []models.Label{}
	for _, protoLabel := range request.Labels {
		label := models.Label{}
		labels = append(labels, *label.FromProtoModel(protoLabel))
	}

	projectID, err := uuid.Parse(request.ProjectId)
	if err != nil {
		log.Errorln(err.Error())
		return "", err
	}

	bucket, err := create.S3Handler.CreateBucket(datasetID)
	if err != nil {
		log.Println(err.Error())
		return "", err
	}

	metadataObjects := make([]models.Object, len(request.GetMetadataObjects()))
	for i, metadataObjectProto := range request.GetMetadataObjects() {
		objectID := uuid.New()

		labels := []models.Label{}
		for _, protoLabel := range request.Labels {
			label := models.Label{}
			labels = append(labels, *label.FromProtoModel(protoLabel))
		}

		location := create.S3Handler.CreateLocation(projectID, datasetID, objectID, metadataObjectProto.Filename, bucket)

		metadataObject := models.Object{
			Filename:   metadataObjectProto.Filename,
			Filetype:   metadataObjectProto.Filetype,
			ContentLen: metadataObjectProto.GetContentLen(),
			Status:     v1storagemodels.Status_STATUS_INITIATING.String(),
			Location:   location,
			DatasetID:  datasetID,
			ProjectID:  projectID,
			Labels:     labels,
			Index:      uint64(i),
			ParentID:   datasetID,
		}

		metadataObjects[i] = metadataObject

	}

	dataset := models.Dataset{
		Name:        request.Name,
		Description: request.Description,
		Labels:      labels,
		ProjectID:   projectID,
		IsPublic:    false,
		Status:      v1storagemodels.Status_STATUS_AVAILABLE.String(),
		MetaObjects: metadataObjects,
	}

	dataset.ID = datasetID

	err = crdbgorm.ExecuteTx(context.Background(), create.DB, nil, func(tx *gorm.DB) error {
		return tx.Create(&dataset).Error
	})

	if err != nil {
		log.Println(err.Error())
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

func (create *Create) CreateObjectGroup(request *v1storageservices.CreateObjectGroupRequest, bucket string) (*models.ObjectGroup, error) {
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

	objectGroupRevisionModel, objects, err := create.prepareObjectGroupRevisionForInsert(request, dataset, bucket)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	objectGroup := models.ObjectGroup{
		CurrentRevisionCount: 0,
		DatasetID:            dataset.ID,
		ProjectID:            dataset.ProjectID,
	}

	err = crdbgorm.ExecuteTx(context.Background(), create.DB, nil, func(tx *gorm.DB) error {
		objectGroupRevisionModel.Objects = objects

		tx.Transaction(func(tx *gorm.DB) error {
			if err := tx.Create(&objectGroup).Error; err != nil {
				log.Errorln(err.Error())
				return err
			}
			objectGroupRevisionModel.ObjectGroupID = objectGroup.ID

			if err := tx.Create(&objectGroupRevisionModel).Error; err != nil {
				return err
			}

			return nil
		})

		return nil
	})

	if err != nil {
		log.Println(err.Error())
		return nil, fmt.Errorf("error while creating entries for object group")
	}

	objectGroup.CurrentObjectGroupRevision = objectGroupRevisionModel
	objectGroup.CurrentObjectGroupRevisionID = objectGroupRevisionModel.ID

	return &objectGroup, nil
}

func (create *Create) CreateObjectGroupBatch(batchRequest *v1storageservices.CreateObjectGroupBatchRequest, bucket string) ([]*models.ObjectGroup, error) {
	var objectGroups []*models.ObjectGroup

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
		objectGroupUUID := uuid.New()

		objectGroup := &models.ObjectGroup{
			CurrentRevisionCount: 0,
			DatasetID:            dataset.ID,
			ProjectID:            dataset.ProjectID,
			ObjectGroupRevisions: make([]models.ObjectGroupRevision, 1),
		}

		objectGroup.ID = objectGroupUUID

		objectGroupRevision, objects, err := create.prepareObjectGroupRevisionForInsert(request, dataset, bucket)
		if err != nil {
			log.Println(err.Error())
			return nil, err
		}

		objectGroupRevision.Objects = objects
		objectGroup.ObjectGroupRevisions[0] = objectGroupRevision

		objectGroups = append(objectGroups, objectGroup)
	}

	err = crdbgorm.ExecuteTx(context.Background(), create.DB, nil, func(tx *gorm.DB) error {
		tx.Transaction(func(tx *gorm.DB) error {
			if err = tx.Create(&objectGroups).Error; err != nil {
				return err
			}

			return nil
		})

		return nil
	})

	if err != nil {
		log.Println(err.Error())
		return nil, fmt.Errorf("could not create error group")
	}

	return objectGroups, nil
}

func (create *Create) prepareObjectGroupRevisionForInsert(request *v1storageservices.CreateObjectGroupRequest, dataset *models.Dataset, bucket string) (models.ObjectGroupRevision, []models.Object, error) {
	objectGroupID := uuid.New()

	labels := []models.Label{}
	for _, protoLabel := range request.Labels {
		label := models.Label{}
		labels = append(labels, *label.FromProtoModel(protoLabel))
	}

	objectGroupModel := models.ObjectGroupRevision{
		DatasetID:   dataset.ID,
		ProjectID:   dataset.ProjectID,
		Name:        request.Name,
		Description: request.Description,
		Labels:      labels,
		Generated:   request.Generated.AsTime(),
		Status:      v1storagemodels.Status_STATUS_INITIATING.String(),
	}

	objectGroupModel.ID = objectGroupID

	objects := make([]models.Object, 0)

	for i, protoObject := range request.GetObjects() {
		uuid := uuid.New()
		location := create.S3Handler.CreateLocation(dataset.ProjectID, dataset.ID, uuid, protoObject.Filename, bucket)

		labels := []models.Label{}
		for _, protoLabel := range protoObject.Labels {
			label := models.Label{}
			labels = append(labels, *label.FromProtoModel(protoLabel))
		}

		object := models.Object{
			Filename:   protoObject.Filename,
			Filetype:   protoObject.Filetype,
			ContentLen: protoObject.ContentLen,
			Location:   location,
			Labels:     labels,
			ObjectUUID: uuid,
			ProjectID:  dataset.ProjectID,
			DatasetID:  dataset.ID,
			Index:      uint64(i),
			Status:     v1storagemodels.Status_STATUS_INITIATING.String(),
			ParentID:   objectGroupID,
		}

		objects = append(objects, object)
	}

	return objectGroupModel, objects, nil
}

func (create *Create) CreateDatasetVersion(request *v1storageservices.ReleaseDatasetVersionRequest, projectID uuid.UUID) (uuid.UUID, error) {
	labels := []models.Label{}
	for _, protoLabel := range request.Labels {
		label := models.Label{}
		labels = append(labels, *label.FromProtoModel(protoLabel))
	}

	objectGroupRevisions := make([]models.ObjectGroupRevision, 0)
	var err error
	for _, objectGroupID := range request.ObjectGroupRevisionIds {
		objectGroup := models.ObjectGroupRevision{}

		objectGroup.ID, err = uuid.Parse(objectGroupID)
		if err != nil {
			log.Debug(err.Error())
			return uuid.UUID{}, err
		}

		objectGroupRevisions = append(objectGroupRevisions, objectGroup)
	}

	datasetID, err := uuid.Parse(request.DatasetId)
	if err != nil {
		log.Debug(err.Error())
		return uuid.UUID{}, err
	}

	version := &models.DatasetVersion{
		Name:                 request.Name,
		Labels:               labels,
		Description:          request.Description,
		DatasetID:            datasetID,
		MajorVersion:         uint(request.Version.Major),
		MinorVersion:         uint(request.Version.Minor),
		PatchVersion:         uint(request.Version.Patch),
		Stage:                request.Version.GetStage().String(),
		RevisionVersion:      uint(request.GetVersion().Revision),
		ProjectID:            projectID,
		ObjectGroupRevisions: objectGroupRevisions,
		Status:               v1storagemodels.Status_STATUS_AVAILABLE.String(),
	}

	err = crdbgorm.ExecuteTx(context.Background(), create.DB, nil, func(tx *gorm.DB) error {
		if err := tx.Omit("ObjectGroupRevisions.*").Create(&version).Error; err != nil {
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

func (create *Create) AddUserToProject(request *v1storageservices.AddUserToProjectRequest) error {
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

func (create *Create) CreateAPIToken(request *v1storageservices.CreateAPITokenRequest, userOauth2ID string) (string, error) {
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
