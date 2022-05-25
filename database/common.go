package database

import (
	"github.com/ScienceObjectsDB/CORE-Server/models"
	"github.com/ScienceObjectsDB/CORE-Server/objectstorage"
	v1storagemodels "github.com/ScienceObjectsDB/go-api/sciobjsdb/api/storage/models/v1"
	v1storageservices "github.com/ScienceObjectsDB/go-api/sciobjsdb/api/storage/services/v1"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

// Common Reusable struct for the database handler
// The database handlers are subdivided into CRUD operations
// Each operation usually gets the request and performs all required actions based on that request.
type Common struct {
	DB        *gorm.DB
	S3Handler *objectstorage.S3ObjectStorageHandler
}

func (common *Common) ObjectForInitialInsert(objectrequest *v1storageservices.CreateObjectRequest, projectID, datasetID, objectGroupID uuid.UUID, bucket string, index uint64) (models.Object, error) {
	uuid := uuid.New()
	location := common.S3Handler.CreateLocation(projectID, datasetID, uuid, objectrequest.Filename, bucket)

	labels := []models.Label{}
	for _, protoLabel := range objectrequest.Labels {
		label := models.Label{}
		labels = append(labels, *label.FromProtoModel(protoLabel))
	}

	object := models.Object{
		Filename:        objectrequest.Filename,
		Filetype:        objectrequest.Filetype,
		ContentLen:      objectrequest.ContentLen,
		Locations:       []models.Location{location},
		Labels:          labels,
		ObjectUUID:      uuid,
		ProjectID:       projectID,
		DatasetID:       datasetID,
		Index:           index,
		Status:          v1storagemodels.Status_STATUS_STAGING.String(),
		DefaultLocation: location,
	}

	return object, nil
}
