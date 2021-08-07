package models

import (
	protomodels "github.com/ScienceObjectsDB/go-api/api/models/v1"
	"gorm.io/gorm"
)

type Project struct {
	gorm.Model
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
		Id:          uint64(project.ID),
		Name:        project.Name,
		Description: project.Description,
		Users:       users,
		Labels:      labels,
		Metadata:    metadataList,
	}
}

type User struct {
	gorm.Model
	UserOauth2ID string
	ProjectID    uint
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
	gorm.Model
	Right  string
	UserID uint
}

func (right *UserRight) ToProtoModel() protomodels.Right {
	return protomodels.Right(protomodels.Right_value[right.Right])
}

type APITokenRight struct {
	gorm.Model
	Right      string
	APITokenID uint
}

func (right *APITokenRight) ToProtoModel() protomodels.Right {
	return protomodels.Right(protomodels.Right_value[right.Right])
}

type APIToken struct {
	gorm.Model
	Token     string `gorm:"index"`
	ProjectID uint
	Project   Project
	UserUUID  string `gorm:"index"`
}

func (token *APIToken) ToProtoModel() *protomodels.APIToken {
	apiToken := protomodels.APIToken{
		Id:        uint64(token.ID),
		Token:     token.Token,
		ProjectId: uint64(token.ProjectID),
	}

	return &apiToken
}
