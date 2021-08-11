package database

import (
	"encoding/base64"

	log "github.com/sirupsen/logrus"

	"github.com/ScienceObjectsDB/CORE-Server/models"
	"github.com/ScienceObjectsDB/CORE-Server/util"
	services "github.com/ScienceObjectsDB/go-api/api/services/v1"
	"gorm.io/gorm"
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
		Name:      request.Name,
		Metadata:  metadataList,
		Labels:    labels,
		ProjectID: uint(request.ProjectId),
		IsPublic:  false,
	}

	result := create.DB.Create(&dataset)
	if result.Error != nil {
		log.Println(result.Error.Error())
		return 0, result.Error
	}

	return dataset.ID, nil
}

func (create *Create) CreateObjectGroup(request *services.CreateObjectGroupRequest) (uint, uint, error) {
	dataset := models.Dataset{}

	dataset.ID = uint(request.GetDatasetId())
	result := create.DB.Find(&dataset)
	if result.Error != nil {
		log.Println(result.Error.Error())
		return 0, 0, result.Error
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

	objectGroupModel := models.ObjectGroup{
		DatasetID:       dataset.ID,
		ProjectID:       dataset.ProjectID,
		RevisionCounter: 0,
		Name:            request.Name,
		Metadata:        metadataList,
		Labels:          labels,
	}

	if err := create.DB.Save(&objectGroupModel).Error; err != nil {
		log.Println(err.Error())
		return 0, 0, err
	}

	revision_id := uint(0)
	var err error

	if request.ObjectGroupRevision != nil {
		revision_id, err = create.CreateObjectGroupRevision(request.ObjectGroupRevision, objectGroupModel.ID)
		if err != nil {
			log.Println(err.Error())
			return 0, 0, err
		}
	}

	return objectGroupModel.ID, revision_id, nil
}

func (create *Create) AddObjectGroupRevision(request *services.AddRevisionToObjectGroupRequest) (uint, error) {
	createReq := services.CreateObjectGroupRevisionRequest{
		Objects:  request.GetGroupRevison().Objects,
		Labels:   request.GroupRevison.GetLabels(),
		Metadata: request.GroupRevison.GetMetadata(),
	}

	revisionId, err := create.CreateObjectGroupRevision(&createReq, uint(request.GetObjectGroupId()))
	if err != nil {
		log.Println(err.Error())
	}

	return revisionId, nil
}

func (create *Create) CreateObjectGroupRevision(request *services.CreateObjectGroupRevisionRequest, objectGroupID uint) (uint, error) {
	var revision uint64
	var objectGroup models.ObjectGroup
	objectGroup.ID = objectGroupID

	if err := create.DB.Transaction(func(tx *gorm.DB) error {
		objectGroup.ID = objectGroupID
		if err := tx.First(&objectGroup).Error; err != nil {
			log.Println(err.Error())
			return err
		}

		objectGroup.RevisionCounter++
		revision = objectGroup.RevisionCounter

		if err := tx.Model(&objectGroup).Update("revision_counter", revision).Error; err != nil {
			log.Println(err.Error())
			return err
		}

		return nil
	}); err != nil {
		log.Println(err.Error())
		return 0, err
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

	revision_model := models.ObjectGroupRevision{
		DatasetID:     objectGroup.DatasetID,
		ProjectID:     objectGroup.ProjectID,
		ObjectGroupID: objectGroupID,
		Labels:        labels,
		Metadata:      metadataList,
		ObjectsCount:  int64(len(request.Objects)),
		Revision:      revision,
		Objects:       []models.Object{},
	}

	for _, objectRequest := range request.Objects {
		labels := []models.Label{}
		for _, protoLabel := range objectRequest.Labels {
			label := models.Label{}
			labels = append(labels, *label.FromProtoModel(protoLabel))
		}

		metadataList := []models.Metadata{}
		for _, protoMetadata := range objectRequest.Metadata {
			metadata := &models.Metadata{}
			metadataList = append(metadataList, *metadata.FromProtoModel(protoMetadata))
		}

		location := create.S3Handler.CreateLocation(objectGroup.ProjectID, objectGroup.DatasetID, revision, objectRequest.Filename)

		object := models.Object{
			Filename:      objectRequest.Filename,
			Filetype:      objectRequest.Filetype,
			ContentLen:    objectRequest.ContentLen,
			Labels:        labels,
			Metadata:      metadataList,
			Location:      *location,
			ProjectID:     objectGroup.ProjectID,
			DatasetID:     objectGroup.DatasetID,
			ObjectGroupID: objectGroup.ID,
		}

		revision_model.Objects = append(revision_model.Objects, object)
	}

	insertedRevision := create.DB.Create(&revision_model)
	if insertedRevision.Error != nil {
		return 0, insertedRevision.Error
	}

	return revision_model.ID, nil
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

	revisions := make([]models.ObjectGroupRevision, 0)
	for _, revision := range request.RevisionIds {
		modelRevision := models.ObjectGroupRevision{}
		modelRevision.ID = uint(revision)

		revisions = append(revisions, modelRevision)
	}

	version := &models.DatasetVersion{
		Labels:               labels,
		Metadata:             metadataList,
		Description:          request.Description,
		DatasetID:            uint(request.DatasetId),
		ObjectGroupRevisions: revisions,
		MajorVersion:         uint(request.Version.Major),
		MinorVersion:         uint(request.Version.Minor),
		PatchVersion:         uint(request.Version.Patch),
		Stage:                request.Version.GetStage().String(),
		RevisionVersion:      uint(request.GetVersion().Revision),
		ProjectID:            projectID,
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
