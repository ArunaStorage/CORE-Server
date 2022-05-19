package database

import (
	"context"

	"github.com/ScienceObjectsDB/CORE-Server/models"
	v1storagemodels "github.com/ScienceObjectsDB/go-api/sciobjsdb/api/storage/models/v1"
	v1storageservices "github.com/ScienceObjectsDB/go-api/sciobjsdb/api/storage/services/v1"
	"github.com/cockroachdb/cockroach-go/v2/crdb/crdbgorm"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type Update struct {
	*Common
}

// Adds an upload id to an object for multipart uploads
func (update *Update) AddUploadID(objectID uuid.UUID, uploadID string) error {
	err := crdbgorm.ExecuteTx(context.Background(), update.DB, nil, func(tx *gorm.DB) error {
		return tx.Model(&models.Object{}).Where("id = ?", objectID).Update("upload_id", uploadID).Error
	})

	if err != nil {
		log.Error(err.Error())
		return err
	}

	return nil
}

func (update *Update) UpdateMetadata() error {
	return nil
}

func (update *Update) UpdateLabels() error {
	return nil
}

func (update *Update) UpdateStatus(status v1storagemodels.Status, resourceID uuid.UUID, resourceType v1storagemodels.Resource) error {
	var model interface{}

	switch resourceType {
	case v1storagemodels.Resource_RESOURCE_PROJECT:
		model = models.Project{}
	case v1storagemodels.Resource_RESOURCE_DATASET:
		model = models.Dataset{}
	case v1storagemodels.Resource_RESOURCE_OBJECT_GROUP:
		model = models.ObjectGroup{}
	case v1storagemodels.Resource_RESOURCE_OBJECT:
		model = models.Object{}
	case v1storagemodels.Resource_RESOURCE_DATASET_VERSION:
		model = models.DatasetVersion{}
	}

	err := crdbgorm.ExecuteTx(context.Background(), update.DB, nil, func(tx *gorm.DB) error {
		return tx.Model(model).Where("id = ?", resourceID).Update("status", status.String()).Error
	})

	if err != nil {
		log.Error(err.Error())
		return err
	}

	return nil
}

func (update *Update) FinishObjectGroupRevisionUpload(objectGroupRevisionID uuid.UUID) error {
	objectGroupRevision := &models.ObjectGroupRevision{}
	objectGroupRevision.ID = objectGroupRevisionID

	objectGroup := &models.ObjectGroup{}

	err := crdbgorm.ExecuteTx(context.Background(), update.DB, nil, func(tx *gorm.DB) error {
		tx.Transaction(func(tx *gorm.DB) error {
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(objectGroupRevision).Error; err != nil {
				log.Errorln(err.Error())
				return err
			}

			objectGroup.ID = objectGroupRevision.ObjectGroupID

			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(objectGroup).Error; err != nil {
				log.Errorln(err.Error())
				return err
			}

			if err := tx.Model(objectGroup).Update("current_revision_count", objectGroup.CurrentRevisionCount+1).Error; err != nil {
				log.Errorln(err.Error())
				return err
			}

			objectGroupRevision.Status = v1storagemodels.Status_STATUS_AVAILABLE.String()
			objectGroupRevision.RevisionNumber = objectGroup.CurrentRevisionCount

			if err := tx.Save(objectGroupRevision).Error; err != nil {
				log.Errorln(err.Error())
				return err
			}

			return nil
		})

		return nil
	})

	if err != nil {
		log.Error(err.Error())
		return err
	}

	return nil
}

// UpdateObjectGroup
// Adds a revision to the history of an object group and sets it as current revision
func (update *Update) UpdateObjectGroup(request *v1storageservices.UpdateObjectGroupRequest) (*models.ObjectGroup, error) {
	objectGroupID, err := uuid.Parse(request.Id)
	if err != nil {
		log.Error(err.Error())
		return nil, err
	}

	objectGroupRevisionID, err := uuid.Parse(request.RevisionId)
	if err != nil {
		log.Error(err.Error())
		return nil, err
	}

	var objectGroup *models.ObjectGroup
	objectGroup.ID = objectGroupID

	var objectGroupRevision *models.ObjectGroupRevision
	objectGroupRevision.ID = objectGroupRevisionID

	err = crdbgorm.ExecuteTx(context.Background(), update.DB, nil, func(tx *gorm.DB) error {
		tx.Transaction(func(tx *gorm.DB) error {
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(objectGroup).Error; err != nil {
				log.Errorln(err.Error())
				return err
			}

			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(objectGroupRevision).Error; err != nil {
				log.Errorln(err.Error())
				return err
			}

			if objectGroupRevision.ObjectGroupID != objectGroup.ID {
				return status.Error(codes.InvalidArgument, "Revision object group does not match provided object group")
			}

			if objectGroupRevision.Status != v1storagemodels.Status_STATUS_AVAILABLE.String() {
				return status.Error(codes.InvalidArgument, "Object groups can only be handled with revisions in ")
			}

			if err := tx.Model(objectGroup).Updates(
				map[string]interface{}{
					"current_revision_id": objectGroupRevision.ID,
				}).Error; err != nil {
				log.Errorln(err.Error())
				return err
			}

			return nil
		})
		return nil
	})

	return objectGroup, nil
}

func (update *Update) updateObjects(request *v1storageservices.UpdateObjectsRequests, dataset *models.Dataset, objectGroup *models.ObjectGroup, objectGroupRevision *models.ObjectGroupRevision) ([]models.Object, error) {
	var dataObjects []models.Object
	var err error

	deleteObjectsMap := make(map[string]interface{})
	for _, object := range request.DeleteObjects {
		deleteObjectsMap[object] = struct{}{}
	}

	updateObjectMap := make(map[string]*v1storageservices.UpdateObjectRequest)
	for _, updateObjectRequest := range request.UpdateObject {
		updateObjectMap[updateObjectRequest.GetId()] = updateObjectRequest
	}

	for i, object := range objectGroupRevision.Objects {
		if _, ok := deleteObjectsMap[object.ID.String()]; ok {
			continue
		}

		if _, ok := updateObjectMap[object.ID.String()]; ok {
			object, err = update.updateObject(updateObjectMap[object.ID.String()], objectGroup, dataset.Bucket, uint64(i))
		}

		if err != nil {
			log.Errorln(err.Error())
		}

		dataObjects = append(dataObjects, object)
	}

	for i, addObjectRequest := range request.AddObjects {
		insertObject, err := update.ObjectForInitialInsert(addObjectRequest, objectGroup.ProjectID, objectGroup.DatasetID, objectGroup.ID, dataset.Bucket, uint64(i))
		if err != nil {
			log.Errorln(err.Error())
		}
		dataObjects = append(dataObjects, insertObject)
	}

	return dataObjects, nil
}

func (update *Update) updateObject(request *v1storageservices.UpdateObjectRequest, objectGroup *models.ObjectGroup, bucket string, index uint64) (models.Object, error) {
	var object models.Object
	var err error

	switch updateRequest := request.UpdateObject.(type) {
	case *v1storageservices.UpdateObjectRequest_UpdatedObject:
		object, err = update.Common.ObjectForInitialInsert(updateRequest.UpdatedObject, objectGroup.ProjectID, objectGroup.DatasetID, objectGroup.ID, bucket, index)
		if err != nil {
			log.Errorln(err.Error())
			return models.Object{}, err
		}
	}

	return object, nil
}
