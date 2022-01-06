package models

import (
	"time"

	protomodels "github.com/ScienceObjectsDB/go-api/api/models/v1"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type BaseModel struct {
	ID        uuid.UUID `gorm:"primaryKey;type:uuid"`
	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt gorm.DeletedAt `gorm:"index"`
}

func (base *BaseModel) BeforeCreate(tx *gorm.DB) error {
	id := uuid.New()
	tx.Statement.SetColumn("ID", id)

	return nil
}

type Metadata struct {
	BaseModel
	Name     string
	Metadata string
	ParentID uuid.UUID `gorm:"index"`
}

func (metadata *Metadata) ToProtoModel() *protomodels.Metadata {
	return &protomodels.Metadata{
		Key:      metadata.Name,
		Metadata: []byte(metadata.Metadata),
	}
}

func (metadata *Metadata) FromProtoModel(metadataProto *protomodels.Metadata) *Metadata {
	metadata.Metadata = string(metadataProto.Metadata)
	metadata.Name = metadataProto.Key

	return metadata
}

type Label struct {
	BaseModel
	Key      string
	Value    string
	ParentID uuid.UUID `gorm:"index"`
}

func (label *Label) ToProtoModel() *protomodels.Label {
	return &protomodels.Label{
		Key:   label.Key,
		Value: label.Value,
	}
}

func (label *Label) FromProtoModel(labelProto *protomodels.Label) *Label {
	label.Key = labelProto.Key
	label.Value = labelProto.Value

	return label
}

type Location struct {
	BaseModel
	ObjectID uuid.UUID `gorm:"index"`
	Endpoint string
	Bucket   string
	Key      string
}

func (location *Location) toProtoModel() *protomodels.Location {
	return &protomodels.Location{
		Location: &protomodels.Location_ObjectLocation{
			ObjectLocation: &protomodels.ObjectLocation{
				Bucket: location.Bucket,
				Key:    location.Key,
				Url:    location.Endpoint,
			},
		},
	}
}
