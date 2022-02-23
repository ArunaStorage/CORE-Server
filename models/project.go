package models

import (
	v1storagemodels "github.com/ScienceObjectsDB/go-api/sciobjsdb/api/storage/models/v1"
	"github.com/google/uuid"
)

type Project struct {
	BaseModel
	Description string
	Users       []User `gorm:"constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
	Name        string
	Status      string
	Labels      []Label    `gorm:"many2many:project_labels;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
	Metadata    []Metadata `gorm:"many2many:project_metadata;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
	APIToken    []APIToken `gorm:"constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
	Datasets    []Dataset  `gorm:"constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
}

func (project *Project) ToProtoModel() *v1storagemodels.Project {
	users := []*v1storagemodels.User{}

	for _, user := range project.Users {
		users = append(users, user.ToProtoModel())
	}

	labels := []*v1storagemodels.Label{}
	for _, label := range project.Labels {
		labels = append(labels, label.ToProtoModel())
	}

	metadataList := []*v1storagemodels.Metadata{}
	for _, metadata := range project.Metadata {
		metadataList = append(metadataList, metadata.ToProtoModel())
	}

	return &v1storagemodels.Project{
		Id:          project.ID.String(),
		Name:        project.Name,
		Description: project.Description,
		Users:       users,
		Labels:      labels,
		Metadata:    metadataList,
	}
}

type User struct {
	BaseModel
	UserOauth2ID string
	ProjectID    uuid.UUID
	Project      Project
}

func (user *User) ToProtoModel() *v1storagemodels.User {
	rights := []v1storagemodels.Right{}
	return &v1storagemodels.User{
		UserId: user.UserOauth2ID,
		Rights: rights,
	}
}

type UserRight struct {
	BaseModel
	Right  string
	UserID uuid.UUID
}

func (right *UserRight) ToProtoModel() v1storagemodels.Right {
	return v1storagemodels.Right(v1storagemodels.Right_value[right.Right])
}

type APITokenRight struct {
	BaseModel
	Right      string
	APITokenID uuid.UUID
}

func (right *APITokenRight) ToProtoModel() v1storagemodels.Right {
	return v1storagemodels.Right(v1storagemodels.Right_value[right.Right])
}

type APIToken struct {
	BaseModel
	Token     string    `gorm:"index"`
	ProjectID uuid.UUID `gorm:"index"`
	Project   Project
	UserUUID  uuid.UUID `gorm:"index"`
}

func (token *APIToken) ToProtoModel() *v1storagemodels.APIToken {
	apiToken := v1storagemodels.APIToken{
		Id:        token.ID.String(),
		Token:     token.Token,
		ProjectId: token.ProjectID.String(),
	}

	return &apiToken
}
