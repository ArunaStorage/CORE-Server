package models

import (
	"time"

	v1storagemodels "github.com/ScienceObjectsDB/go-api/sciobjsdb/api/storage/models/v1"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Object struct {
	BaseModel
	ObjectUUID uuid.UUID `gorm:"index,unique"`
	Filename   string    `gorm:"index"`
	Filetype   string
	ContentLen int64
	Status     string   `gorm:"index"`
	Location   Location `gorm:"embedded"`
	Labels     []Label  `gorm:"many2many:object_labels;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
	UploadID   string
	Index      uint64
	ProjectID  uuid.UUID `gorm:"index"`
	Project    Project
	DatasetID  uuid.UUID `gorm:"index"`
	Dataset    Dataset
	ParentID   uuid.UUID `gorm:"index"`
}

func (object *Object) ToProtoModel() (*v1storagemodels.Object, error) {
	labels := []*v1storagemodels.Label{}
	for _, label := range object.Labels {
		labels = append(labels, label.ToProtoModel())
	}

	status, err := ToStatus(object.Status)
	if err != nil {
		log.Errorln(err.Error())
		return nil, err
	}

	return &v1storagemodels.Object{
		Id:            object.ID.String(),
		Filename:      object.Filename,
		Filetype:      object.Filetype,
		Labels:        labels,
		Created:       timestamppb.New(object.CreatedAt),
		Location:      object.Location.toProtoModel(),
		ContentLen:    object.ContentLen,
		UploadId:      object.UploadID,
		DatasetId:     object.DatasetID.String(),
		ProjectId:     object.ProjectID.String(),
		ObjectGroupId: object.ParentID.String(),
		Status:        status,
	}, nil

}

type ObjectGroup struct {
	BaseModel
	Name            string
	Description     string
	DatasetID       uuid.UUID
	Dataset         Dataset
	ProjectID       uuid.UUID `gorm:"index"`
	Project         Project
	Labels          []Label          `gorm:"many2many:object_group_label;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
	DatasetVersions []DatasetVersion `gorm:"many2many:dataset_version_object_groups;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
	Status          string           `gorm:"index"`
	Generated       time.Time
	Objects         []Object `gorm:"many2many:object_group_data_objects;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
	MetaObjects     []Object `gorm:"many2many:object_group_meta_objects;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
}

func (objectGroup *ObjectGroup) ToProtoModel(stats *v1storagemodels.ObjectGroupStats) (*v1storagemodels.ObjectGroup, error) {
	labels := []*v1storagemodels.Label{}
	for _, label := range objectGroup.Labels {
		labels = append(labels, label.ToProtoModel())
	}

	objectsList := make([]*v1storagemodels.Object, len(objectGroup.Objects))
	for _, object := range objectGroup.Objects {

		protoObject, err := object.ToProtoModel()
		if err != nil {
			log.Errorln(err)
			return nil, err
		}
		objectsList[object.Index] = protoObject
	}

	metaObjectsList := make([]*v1storagemodels.Object, len(objectGroup.MetaObjects))
	for _, metaObject := range objectGroup.MetaObjects {

		protoObject, err := metaObject.ToProtoModel()
		if err != nil {
			log.Errorln(err)
			return nil, err
		}
		objectsList[metaObject.Index] = protoObject
	}

	status, err := ToStatus(objectGroup.Status)
	if err != nil {
		log.Errorln(err)
		return nil, err
	}

	return &v1storagemodels.ObjectGroup{
		Id:              objectGroup.ID.String(),
		Name:            objectGroup.Name,
		Description:     objectGroup.Description,
		DatasetId:       objectGroup.DatasetID.String(),
		ProjectId:       objectGroup.ProjectID.String(),
		Labels:          labels,
		Objects:         objectsList,
		Created:         timestamppb.New(objectGroup.CreatedAt),
		Generated:       timestamppb.New(objectGroup.Generated),
		MetadataObjects: metaObjectsList,
		Status:          status,
		Stats:           stats,
	}, nil
}
