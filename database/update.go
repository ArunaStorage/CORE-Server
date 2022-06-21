package database

import (
	"context"
	"fmt"

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

func (update *Update) FinishObjectUpload(objectID uuid.UUID) error {
	object := &models.Object{}
	object.ID = objectID

	err := crdbgorm.ExecuteTx(context.Background(), update.DB, nil, func(tx *gorm.DB) error {
		tx.Transaction(func(tx *gorm.DB) error {
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(object).Error; err != nil {
				log.Errorln(err.Error())
				return err
			}

			if object.Status != v1storagemodels.Status_STATUS_STAGING.String() {
				err := status.Error(codes.InvalidArgument, fmt.Sprintf("object is in status: %v but finishing upload requires object to be in status: %v", object.Status, v1storagemodels.Status_STATUS_STAGING))
				log.Debugln(err.Error())
				return err
			}

			if err := tx.Model(object).Update("status", v1storagemodels.Status_STATUS_AVAILABLE.String()).Error; err != nil {
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

func (update *Update) UpdateObjectGroup(request *v1storageservices.UpdateObjectGroupRequest, dataset *models.Dataset, project *models.Project, objectGroup *models.ObjectGroup) (*models.ObjectGroupRevision, error) {
	newObjectGroupRevision := &models.ObjectGroupRevision{
		Name:          request.CreateRevisionRequest.Name,
		Description:   request.CreateRevisionRequest.Description,
		DatasetID:     dataset.ID,
		ProjectID:     project.ID,
		ObjectGroupID: objectGroup.ID,
	}

	err := crdbgorm.ExecuteTx(context.Background(), update.DB, nil, func(tx *gorm.DB) error {

		tx.Transaction(func(tx *gorm.DB) error {
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(objectGroup).Error; err != nil {
				log.Errorln(err.Error())
				return err
			}

			currentObjectGroupInTransaction := &models.ObjectGroup{}
			currentObjectGroupInTransaction.ID = objectGroup.ID

			if err := tx.First(currentObjectGroupInTransaction).Error; err != nil {
				log.Errorln(err.Error())
				return err
			}

			dataObjects := make([]models.Object, 0)

			dataObjectPreloads := tx.Preload("Locations").Preload("DefaultLocation").Preload("Labels")
			dataObjectPreloads.Model(&models.Object{}).
				Joins("inner join object_group_revision_data_objects on object_group_revision_data_objects.object_id = objects.id").
				Where("object_group_revision_id = ?", currentObjectGroupInTransaction.CurrentObjectGroupRevisionID).
				Find(&dataObjects)

			metaObjects := make([]models.Object, 0)

			metaObjectsPreloads := tx.Preload("Locations").Preload("DefaultLocation").Preload("Labels")
			metaObjectsPreloads.Model(&models.Object{}).
				Joins("inner join object_group_revision_meta_objects on object_group_revision_meta_objects.object_id = objects.id").
				Where("object_group_revision_id = ?", currentObjectGroupInTransaction.CurrentObjectGroupRevisionID).
				Find(&metaObjects)

			currentObjectGroupRevision := &models.ObjectGroupRevision{}
			currentObjectGroupRevision.ID = currentObjectGroupInTransaction.CurrentObjectGroupRevisionID
			if err := tx.First(currentObjectGroupRevision).Error; err != nil {
				log.Errorln(err.Error())
				return err
			}

			new_data_objects, err := update.updateObjects(dataObjects, request.CreateRevisionRequest.UpdateObjects)
			if err != nil {
				log.Errorln(err.Error())
				return err
			}

			//new_meta_objects, err := update.updateObjects(metaObjects, request.CreateRevisionRequest.UpdateMetaObjects)
			//if err != nil {
			//	log.Errorln(err.Error())
			//	return err
			//}

			labels := make([]models.Label, len(request.CreateRevisionRequest.Labels))
			for i, labelRequest := range labels {
				labels[i] = models.Label{
					Key:   labelRequest.Key,
					Value: labelRequest.Value,
				}
			}

			newObjectGroupRevision.DataObjects = new_data_objects
			//newObjectGroupRevision.MetaObjects = new_meta_objects
			newObjectGroupRevision.Labels = labels
			newObjectGroupRevision.RevisionNumber = objectGroup.CurrentRevisionCount + 1

			if err := tx.Create(newObjectGroupRevision).Error; err != nil {
				log.Errorln(err.Error())
				return err
			}

			updateColumns := map[string]interface{}{"current_object_group_revision_id": newObjectGroupRevision.ID.String(), "current_revision_count": objectGroup.CurrentRevisionCount + 1}
			if err := tx.Model(objectGroup).Updates(updateColumns).Error; err != nil {
				log.Errorln(err.Error())
				return err
			}

			return nil
		})

		return nil
	})

	if err != nil {
		log.Errorln(err.Error())
		return nil, err
	}

	return newObjectGroupRevision, nil
}

func (update *Update) updateObjects(originalObjects []models.Object, updateObjectsRequest *v1storageservices.UpdateObjectsRequests) ([]models.Object, error) {
	deleteObjects := make(map[string]interface{})

	for _, deleteObject := range updateObjectsRequest.GetDeleteObjects() {
		deleteObjects[deleteObject.GetId()] = struct{}{}
	}

	newDataObjects := make([]models.Object, 0)
	for _, originalObject := range originalObjects {
		if _, ok := deleteObjects[originalObject.ID.String()]; !ok {
			newDataObjects = append(newDataObjects, originalObject)
		}
	}

	for _, addObjectRequest := range updateObjectsRequest.GetAddObjects() {
		addObjectUUID, err := uuid.Parse(addObjectRequest.GetId())
		if err != nil {
			log.Errorln(err.Error())
			return nil, err
		}

		addObject := models.Object{}
		addObject.ID = addObjectUUID

		newDataObjects = append(newDataObjects, addObject)
	}

	return newDataObjects, nil
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

			objectGroup.CurrentObjectGroupRevisionID = objectGroupRevision.ID

			if err := tx.Model(objectGroup).Update("current_object_group_revision_id", objectGroupRevision.ID).Error; err != nil {
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
