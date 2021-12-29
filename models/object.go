package models

import (
	"time"

	protomodels "github.com/ScienceObjectsDB/go-api/api/models/v1"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gorm.io/gorm"
)

type Object struct {
	ID            uuid.UUID `gorm:"primaryKey;type:uuid;default:uuid_generate_v4()"`
	CreatedAt     time.Time
	UpdatedAt     time.Time
	DeletedAt     gorm.DeletedAt `gorm:"index"`
	ObjectUUID    uuid.UUID      `gorm:"index,unique"`
	Filename      string         `gorm:"index"`
	Filetype      string
	ContentLen    int64
	Location      Location   `gorm:"foreignKey:ObjectID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
	Labels        []Label    `gorm:"many2many:object_labels;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
	Metadata      []Metadata `gorm:"many2many:object_metadata;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
	UploadID      string
	Index         uint64
	ProjectID     uuid.UUID `gorm:"index"`
	Project       Project
	DatasetID     uuid.UUID `gorm:"index"`
	Dataset       Dataset
	ObjectGroupID uuid.UUID `gorm:"index"`
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
		Id:         object.ID.String(),
		Filename:   object.Filename,
		Filetype:   object.Filetype,
		Labels:     labels,
		Metadata:   metadataList,
		Created:    timestamppb.New(object.CreatedAt),
		Location:   object.Location.toProtoModel(),
		ContentLen: object.ContentLen,
		UploadId:   object.UploadID,
		DatasetId:  object.DatasetID.String(),
		ProjectId:  object.ProjectID.String(),
	}

}

type ObjectGroup struct {
	ID              uuid.UUID `gorm:"primaryKey;type:uuid;default:uuid_generate_v4()"`
	CreatedAt       time.Time
	UpdatedAt       time.Time
	DeletedAt       gorm.DeletedAt `gorm:"index"`
	Name            string         `gorm:"index:unique_group_name,unique"`
	Description     string
	DatasetID       uuid.UUID `gorm:"index;index:unique_group_name,unique"`
	Dataset         Dataset
	ProjectID       uuid.UUID `gorm:"index"`
	Project         Project
	Labels          []Label          `gorm:"many2many:object_group_label;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
	Metadata        []Metadata       `gorm:"many2many:object_group_metadata;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
	DatasetVersions []DatasetVersion `gorm:"many2many:dataset_version_object_groups;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
	Status          string
	Generated       time.Time
	Objects         []Object `gorm:"constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
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

	objectsList := make([]*protomodels.Object, len(objectGroup.Objects))
	for _, object := range objectGroup.Objects {
		objectsList[object.Index] = object.ToProtoModel()
	}

	return &protomodels.ObjectGroup{
		Id:          objectGroup.ID.String(),
		Name:        objectGroup.Name,
		Description: objectGroup.Description,
		DatasetId:   objectGroup.DatasetID.String(),
		ProjectId:   objectGroup.ProjectID.String(),
		Labels:      labels,
		Metadata:    metadataList,
		Objects:     objectsList,
		Created:     timestamppb.New(objectGroup.CreatedAt),
		Generated:   timestamppb.New(objectGroup.Generated),
	}
}
