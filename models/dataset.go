package models

import (
	protomodels "github.com/ScienceObjectsDB/go-api/api/models/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gorm.io/gorm"
)

type Dataset struct {
	gorm.Model
	Name            string
	Description     string
	IsPublic        bool
	Status          string
	Labels          []Label    `gorm:"many2many:dataset_labels;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
	Metadata        []Metadata `gorm:"many2many:dataset_metadata;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
	ProjectID       uint
	Project         Project
	ObjectGroups    []ObjectGroup    `gorm:"constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
	DatasetVersions []DatasetVersion `gorm:"constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
}

func (dataset *Dataset) ToProtoModel() protomodels.Dataset {
	labels := []*protomodels.Label{}
	for _, label := range dataset.Labels {
		labels = append(labels, label.ToProtoModel())
	}

	metadataList := []*protomodels.Metadata{}
	for _, metadata := range dataset.Metadata {
		metadataList = append(metadataList, metadata.ToProtoModel())
	}

	return protomodels.Dataset{
		Id:          uint64(dataset.ID),
		Name:        dataset.Name,
		Description: dataset.Description,
		Created:     timestamppb.New(dataset.CreatedAt),
		Labels:      labels,
		Metadata:    metadataList,
		ProjectId:   uint64(dataset.ProjectID),
		IsPublic:    dataset.IsPublic,
	}
}

type DatasetVersion struct {
	gorm.Model
	Name                 string
	Description          string
	Labels               []Label               `gorm:"many2many:dataset_version_labels;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
	Metadata             []Metadata            `gorm:"many2many:dataset_version_metadata;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
	ObjectGroupRevisions []ObjectGroupRevision `gorm:"many2many:dataset_version_object_group_revisions;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
	MajorVersion         uint
	MinorVersion         uint
	PatchVersion         uint
	RevisionVersion      uint
	Stage                string
	ProjectID            uint
	Project              Project
	DatasetID            uint
	Dataset              Dataset
}

func (version *DatasetVersion) ToProtoModel() *protomodels.DatasetVersion {
	labels := []*protomodels.Label{}
	for _, label := range version.Labels {
		labels = append(labels, label.ToProtoModel())
	}

	metadataList := []*protomodels.Metadata{}
	for _, metadata := range version.Metadata {
		metadataList = append(metadataList, metadata.ToProtoModel())
	}

	var objectGroupIDs []uint64
	for _, id := range version.ObjectGroupRevisions {
		objectGroupIDs = append(objectGroupIDs, uint64(id.ID))
	}

	versionTag := protomodels.Version_VersionStage_value[version.Stage]

	protoVersion := &protomodels.DatasetVersion{
		Id:          uint64(version.ID),
		DatasetId:   uint64(version.DatasetID),
		Description: version.Description,
		Labels:      labels,
		Metadata:    metadataList,
		Created:     timestamppb.New(version.CreatedAt),
		Version: &protomodels.Version{
			Major:    int32(version.MajorVersion),
			Minor:    int32(version.MinorVersion),
			Patch:    int32(version.PatchVersion),
			Revision: int32(version.RevisionVersion),
			Stage:    protomodels.Version_VersionStage(versionTag),
		},
		ObjectGroupIds: objectGroupIDs,
		Name:           version.Name,
	}

	return protoVersion
}
