package models

import (
	"time"

	protomodels "github.com/ScienceObjectsDB/go-api/api/models/v1"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gorm.io/gorm"
)

type Dataset struct {
	ID              uuid.UUID `gorm:"type:uuid;default:uuid_generate_v4()"`
	CreatedAt       time.Time
	UpdatedAt       time.Time
	DeletedAt       gorm.DeletedAt `gorm:"index"`
	Name            string
	Description     string
	IsPublic        bool
	Status          string
	Labels          []Label    `gorm:"many2many:dataset_labels;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
	Metadata        []Metadata `gorm:"many2many:dataset_metadata;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
	ProjectID       uuid.UUID  `gorm:"index"`
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
		Id:          dataset.ID.String(),
		Name:        dataset.Name,
		Description: dataset.Description,
		Created:     timestamppb.New(dataset.CreatedAt),
		Labels:      labels,
		Metadata:    metadataList,
		ProjectId:   dataset.ProjectID.String(),
		IsPublic:    dataset.IsPublic,
	}
}

type DatasetVersion struct {
	ID              uuid.UUID `gorm:"type:uuid;default:uuid_generate_v4()"`
	CreatedAt       time.Time
	UpdatedAt       time.Time
	DeletedAt       gorm.DeletedAt `gorm:"index"`
	Name            string
	Description     string
	Labels          []Label       `gorm:"many2many:dataset_version_labels;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
	Metadata        []Metadata    `gorm:"many2many:dataset_version_metadata;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
	ObjectGroups    []ObjectGroup `gorm:"many2many:dataset_version_object_groups;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
	MajorVersion    uint
	MinorVersion    uint
	PatchVersion    uint
	RevisionVersion uint
	Stage           string
	ProjectID       uuid.UUID `gorm:"index"`
	Project         Project
	DatasetID       uuid.UUID `gorm:"index"`
	Dataset         Dataset
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

	var objectGroupIDs []string
	for _, id := range version.ObjectGroups {
		objectGroupIDs = append(objectGroupIDs, id.ID.String())
	}

	versionTag := protomodels.Version_VersionStage_value[version.Stage]

	protoVersion := &protomodels.DatasetVersion{
		Id:          version.ID.String(),
		DatasetId:   version.DatasetID.String(),
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
