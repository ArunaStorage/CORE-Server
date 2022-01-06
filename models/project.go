package models

import (
	protomodels "github.com/ScienceObjectsDB/go-api/api/models/v1"
	"github.com/google/uuid"
)

type Project struct {
	BaseModel
	Description string
	Users       []User `gorm:"constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
	Name        string
	Labels      []Label    `gorm:"many2many:project_labels;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
	Metadata    []Metadata `gorm:"many2many:project_metadata;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
	APIToken    []APIToken `gorm:"constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
	Datasets    []Dataset  `gorm:"constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
}

func (project *Project) ToProtoModel() *protomodels.Project {
	users := []*protomodels.User{}

	for _, user := range project.Users {
		users = append(users, user.ToProtoModel())
	}

	labels := []*protomodels.Label{}
	for _, label := range project.Labels {
		labels = append(labels, label.ToProtoModel())
	}

	metadataList := []*protomodels.Metadata{}
	for _, metadata := range project.Metadata {
		metadataList = append(metadataList, metadata.ToProtoModel())
	}

	return &protomodels.Project{
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

func (user *User) ToProtoModel() *protomodels.User {
	rights := []protomodels.Right{}
	return &protomodels.User{
		UserId: user.UserOauth2ID,
		Rights: rights,
	}
}

type UserRight struct {
	BaseModel
	Right  string
	UserID uuid.UUID
}

func (right *UserRight) ToProtoModel() protomodels.Right {
	return protomodels.Right(protomodels.Right_value[right.Right])
}

type APITokenRight struct {
	BaseModel
	Right      string
	APITokenID uuid.UUID
}

func (right *APITokenRight) ToProtoModel() protomodels.Right {
	return protomodels.Right(protomodels.Right_value[right.Right])
}

type APIToken struct {
	BaseModel
	Token     string    `gorm:"index"`
	ProjectID uuid.UUID `gorm:"index"`
	Project   Project
	UserUUID  uuid.UUID `gorm:"index"`
}

func (token *APIToken) ToProtoModel() *protomodels.APIToken {
	apiToken := protomodels.APIToken{
		Id:        token.ID.String(),
		Token:     token.Token,
		ProjectId: token.ProjectID.String(),
	}

	return &apiToken
}
