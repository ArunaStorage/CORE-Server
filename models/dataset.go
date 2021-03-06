package models

import (
	v1storagemodels "github.com/ScienceObjectsDB/go-api/sciobjsdb/api/storage/models/v1"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Dataset struct {
	BaseModel
	Name            string `gorm:"index"`
	Description     string
	Bucket          string
	IsPublic        bool
	Status          string    `gorm:"index"`
	Labels          []Label   `gorm:"many2many:dataset_labels;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
	ProjectID       uuid.UUID `gorm:"index"`
	Project         Project
	ObjectGroups    []ObjectGroup    `gorm:"constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
	DatasetVersions []DatasetVersion `gorm:"constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
	MetaObjects     []Object         `gorm:"many2many:dataset_meta_objects;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
}

func (dataset *Dataset) ToProtoModel(stats *v1storagemodels.DatasetStats) (*v1storagemodels.Dataset, error) {
	labels := []*v1storagemodels.Label{}
	for _, label := range dataset.Labels {
		labels = append(labels, label.ToProtoModel())
	}

	metaObjectsList := make([]*v1storagemodels.Object, len(dataset.MetaObjects))
	for i, metaObject := range dataset.MetaObjects {

		protoObject, err := metaObject.ToProtoModel()
		if err != nil {
			log.Errorln(err)
			return nil, err
		}
		metaObjectsList[i] = protoObject
	}

	status, err := ToStatus(dataset.Status)
	if err != nil {
		log.Debugln(err)
		return nil, err
	}

	return &v1storagemodels.Dataset{
		Id:              dataset.ID.String(),
		Name:            dataset.Name,
		Description:     dataset.Description,
		Created:         timestamppb.New(dataset.CreatedAt),
		Labels:          labels,
		ProjectId:       dataset.ProjectID.String(),
		IsPublic:        dataset.IsPublic,
		Bucket:          dataset.Bucket,
		Status:          status,
		Stats:           stats,
		MetadataObjects: metaObjectsList,
	}, nil
}

type DatasetVersion struct {
	BaseModel
	Name                 string
	Description          string
	Labels               []Label               `gorm:"many2many:dataset_version_labels;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
	ObjectGroupRevisions []ObjectGroupRevision `gorm:"many2many:dataset_version_object_group_revisions;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
	MajorVersion         uint                  `gorm:"index"`
	MinorVersion         uint                  `gorm:"index"`
	PatchVersion         uint                  `gorm:"index"`
	RevisionVersion      uint                  `gorm:"index"`
	Stage                string                `gorm:"index"`
	ProjectID            uuid.UUID             `gorm:"index"`
	Project              Project
	DatasetID            uuid.UUID `gorm:"index"`
	Dataset              Dataset
	Status               string
}

func (version *DatasetVersion) ToProtoModel(stats *v1storagemodels.DatasetVersionStats) (*v1storagemodels.DatasetVersion, error) {
	labels := []*v1storagemodels.Label{}
	for _, label := range version.Labels {
		labels = append(labels, label.ToProtoModel())
	}

	var objectGroupIDs []string
	for _, id := range version.ObjectGroupRevisions {
		objectGroupIDs = append(objectGroupIDs, id.ID.String())
	}

	versionTag := v1storagemodels.Version_VersionStage_value[version.Stage]

	status, err := ToStatus(version.Status)
	if err != nil {
		log.Errorln(err.Error())
		return nil, err
	}

	protoVersion := &v1storagemodels.DatasetVersion{
		Id:          version.ID.String(),
		DatasetId:   version.DatasetID.String(),
		Description: version.Description,
		Labels:      labels,
		Created:     timestamppb.New(version.CreatedAt),
		Version: &v1storagemodels.Version{
			Major:    int32(version.MajorVersion),
			Minor:    int32(version.MinorVersion),
			Patch:    int32(version.PatchVersion),
			Revision: int32(version.RevisionVersion),
			Stage:    v1storagemodels.Version_VersionStage(versionTag),
		},
		ObjectGroupIds: objectGroupIDs,
		Name:           version.Name,
		Status:         status,
		Stats:          stats,
	}

	return protoVersion, nil
}
