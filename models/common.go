package models

import (
	protomodels "github.com/ScienceObjectsDB/go-api/api/models/v1"
	"gorm.io/gorm"
)

type Metadata struct {
	gorm.Model
	Name     string
	Metadata string
	ParentID uint `gorm:"index"`
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
	gorm.Model
	Key   string
	Value string
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
	gorm.Model
	ObjectID uint `gorm:"index"`
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
