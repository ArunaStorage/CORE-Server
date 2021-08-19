package models

import (
	"time"

	protomodels "github.com/ScienceObjectsDB/go-api/api/models/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gorm.io/gorm"
)

type Object struct {
	gorm.Model
	ObjectUUID    string `gorm:"index,unique"`
	Filename      string
	Filetype      string
	ContentLen    int64
	Location      Location   `gorm:"foreignKey:ObjectID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
	Labels        []Label    `gorm:"many2many:object_labels;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
	Metadata      []Metadata `gorm:"many2many:object_metadata;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
	UploadID      string
	ProjectID     uint
	Project       Project
	DatasetID     uint
	Dataset       Dataset
	ObjectGroupID uint
	ObjectGroup   ObjectGroup
}

func (object *Object) ToProtoModel() *protomodels.Object {
	labels := []*protomodels.Label{}
	for _, label := range object.Labels {
		labels = append(labels, label.ToProtoModel())
	}

	metadataList := []*protomodels.Metadata{}
	for _, metadata := range object.Metadata {
		metadataList = append(metadataList, metadata.ToProtoModel())
	}

	return &protomodels.Object{
		Id:         uint64(object.ID),
		Filename:   object.Filename,
		Filetype:   object.Filetype,
		Labels:     labels,
		Metadata:   metadataList,
		Created:    timestamppb.New(object.CreatedAt),
		Location:   object.Location.toProtoModel(),
		ContentLen: object.ContentLen,
		UploadId:   object.UploadID,
	}

}

type ObjectGroup struct {
	gorm.Model
	Name        string
	Description string
	DatasetID   uint
	Dataset     Dataset
	ProjectID   uint
	Project     Project
	Labels      []Label    `gorm:"many2many:object_group_label;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
	Metadata    []Metadata `gorm:"many2many:object_group_metadata;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
	Status      string
	Generated   time.Time
	Objects     []Object `gorm:"constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
}

func (objectGroup *ObjectGroup) ToProtoModel() *protomodels.ObjectGroup {
	labels := []*protomodels.Label{}
	for _, label := range objectGroup.Labels {
		labels = append(labels, label.ToProtoModel())
	}

	metadataList := []*protomodels.Metadata{}
	for _, metadata := range objectGroup.Metadata {
		metadataList = append(metadataList, metadata.ToProtoModel())
	}

	objectsList := []*protomodels.Object{}
	for _, object := range objectGroup.Objects {
		objectsList = append(objectsList, object.ToProtoModel())
	}

	return &protomodels.ObjectGroup{
		Id:          uint64(objectGroup.ID),
		Name:        objectGroup.Name,
		Description: objectGroup.Description,
		DatasetId:   uint64(objectGroup.DatasetID),
		Labels:      labels,
		Metadata:    metadataList,
		Objects:     objectsList,
	}
}
