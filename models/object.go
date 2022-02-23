package models

import (
	"time"

	v1storagemodels "github.com/ScienceObjectsDB/go-api/sciobjsdb/api/storage/models/v1"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Object struct {
	BaseModel
	ObjectUUID    uuid.UUID `gorm:"index,unique"`
	Filename      string    `gorm:"index"`
	Filetype      string
	ContentLen    int64
	Status        string
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

func (object *Object) ToProtoModel() *v1storagemodels.Object {
	labels := []*v1storagemodels.Label{}
	for _, label := range object.Labels {
		labels = append(labels, label.ToProtoModel())
	}

	metadataList := []*v1storagemodels.Metadata{}
	for _, metadata := range object.Metadata {
		metadataList = append(metadataList, metadata.ToProtoModel())
	}

	return &v1storagemodels.Object{
		Id:            object.ID.String(),
		Filename:      object.Filename,
		Filetype:      object.Filetype,
		Labels:        labels,
		Metadata:      metadataList,
		Created:       timestamppb.New(object.CreatedAt),
		Location:      object.Location.toProtoModel(),
		ContentLen:    object.ContentLen,
		UploadId:      object.UploadID,
		DatasetId:     object.DatasetID.String(),
		ProjectId:     object.ProjectID.String(),
		ObjectGroupId: object.ObjectGroupID.String(),
	}

}

type ObjectGroup struct {
	BaseModel
	Name            string `gorm:"index:unique_group_name,unique"`
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

func (objectGroup *ObjectGroup) ToProtoModel() *v1storagemodels.ObjectGroup {
	labels := []*v1storagemodels.Label{}
	for _, label := range objectGroup.Labels {
		labels = append(labels, label.ToProtoModel())
	}

	metadataList := []*v1storagemodels.Metadata{}
	for _, metadata := range objectGroup.Metadata {
		metadataList = append(metadataList, metadata.ToProtoModel())
	}

	objectsList := make([]*v1storagemodels.Object, len(objectGroup.Objects))
	for _, object := range objectGroup.Objects {
		objectsList[object.Index] = object.ToProtoModel()
	}

	return &v1storagemodels.ObjectGroup{
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
