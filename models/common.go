package models

import (
	"fmt"
	"time"

	v1storagemodels "github.com/ScienceObjectsDB/go-api/sciobjsdb/api/storage/models/v1"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

type BaseModel struct {
	ID        uuid.UUID `gorm:"primaryKey;type:uuid"`
	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt gorm.DeletedAt `gorm:"index"`
}

func (base *BaseModel) BeforeCreate(tx *gorm.DB) error {
	if base.ID == uuid.Nil {
		id := uuid.New()
		tx.Statement.SetColumn("ID", id)
	}

	return nil
}

type Label struct {
	BaseModel
	Key      string
	Value    string
	ParentID uuid.UUID `gorm:"index"`
}

func (label *Label) ToProtoModel() *v1storagemodels.Label {
	return &v1storagemodels.Label{
		Key:   label.Key,
		Value: label.Value,
	}
}

func (label *Label) FromProtoModel(labelProto *v1storagemodels.Label) *Label {
	label.Key = labelProto.Key
	label.Value = labelProto.Value

	return label
}

type Location struct {
	BaseModel
	Endpoint  string
	Bucket    string
	Key       string
	UploadID  string
	Status    string
	ProjectID uuid.UUID `gorm:"index"`
	Project   Project   `gorm:"constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
	DatasetID uuid.UUID `gorm:"index"`
	Dataset   Dataset   `gorm:"constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
	ObjectID  uuid.UUID `gorm:"index"`
}

func (location *Location) toProtoModel() (*v1storagemodels.Location, error) {
	protoStatus, err := ToStatus(location.Status)
	if err != nil {
		log.Errorln(err.Error())
		return nil, err
	}

	return &v1storagemodels.Location{
		ObjectLocation: &v1storagemodels.ObjectLocation{
			Id:       location.ID.String(),
			Bucket:   location.Bucket,
			Key:      location.Key,
			Url:      location.Endpoint,
			Status:   protoStatus,
			UploadId: location.UploadID,
		},
	}, nil
}

func ToStatus(status string) (v1storagemodels.Status, error) {

	var statusEnum v1storagemodels.Status

	if val, ok := v1storagemodels.Status_value[status]; !ok {
		err := fmt.Errorf("status %v not recognized", status)
		log.Debug(err)
		statusEnum = v1storagemodels.Status_STATUS_UNSPECIFIED
	} else {
		statusEnum = v1storagemodels.Status(val)
	}

	return statusEnum, nil
}
