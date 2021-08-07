package models

import (
	protomodels "github.com/ScienceObjectsDB/go-api/api/models/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gorm.io/gorm"
)

type Object struct {
	gorm.Model
	Filename              string
	Filetype              string
	ContentLen            int64
	Location              Location   `gorm:"foreignKey:ObjectID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
	Labels                []Label    `gorm:"many2many:object_labels;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
	Metadata              []Metadata `gorm:"many2many:object_metadata;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
	UploadID              string
	ProjectID             uint
	Project               Project
	DatasetID             uint
	Dataset               Dataset
	ObjectGroupID         uint
	ObjectGroupRevisionID *uint
	ObjectGroupRevision   ObjectGroupRevision
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

type ObjectGroupRevision struct {
	gorm.Model
	DatasetID     uint
	Dataset       Dataset
	ProjectID     uint
	Project       Project
	ObjectGroupID uint
	Labels        []Label    `gorm:"many2many:object_group_revision_label;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
	Metadata      []Metadata `gorm:"many2many:object_group_revision_metadata;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
	ObjectsCount  int64
	Revision      uint64
	Status        string
	Objects       []Object `gorm:"constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
}

func (objectGroupRevision *ObjectGroupRevision) ToProtoModel() *protomodels.ObjectGroupRevision {
	labels := []*protomodels.Label{}
	for _, label := range objectGroupRevision.Labels {
		labels = append(labels, label.ToProtoModel())
	}

	metadataList := []*protomodels.Metadata{}
	for _, metadata := range objectGroupRevision.Metadata {
		metadataList = append(metadataList, metadata.ToProtoModel())
	}

	objects := []*protomodels.Object{}
	for _, object := range objectGroupRevision.Objects {
		objects = append(objects, object.ToProtoModel())
	}

	return &protomodels.ObjectGroupRevision{
		Id:        uint64(objectGroupRevision.ID),
		DatasetId: uint64(objectGroupRevision.DatasetID),
		Created:   timestamppb.New(objectGroupRevision.CreatedAt),
		Labels:    labels,
		Metadata:  metadataList,
		Objects:   objects,
		Revision:  int64(objectGroupRevision.Revision),
	}
}

type ObjectGroup struct {
	gorm.Model
	Name                 string
	DatasetID            uint
	Dataset              Dataset
	ProjectID            uint
	Project              Project
	Labels               []Label    `gorm:"many2many:object_group_label;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
	Metadata             []Metadata `gorm:"many2many:object_group_metadata;onstraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
	Status               string
	HeadID               uint
	RevisionCounter      uint64
	ObjectGroupRevisions []ObjectGroupRevision `gorm:"constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
	Objects              []Object              `gorm:"constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
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

	return &protomodels.ObjectGroup{
		Id:              uint64(objectGroup.ID),
		Name:            objectGroup.Name,
		DatasetId:       uint64(objectGroup.DatasetID),
		Labels:          labels,
		Metadata:        metadataList,
		HeadId:          uint64(objectGroup.HeadID),
		CurrentRevision: int64(objectGroup.RevisionCounter),
	}
}
