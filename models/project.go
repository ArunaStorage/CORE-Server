package models

import (
	"time"

	"gorm.io/gorm"

	v1storagemodels "github.com/ScienceObjectsDB/go-api/sciobjsdb/api/storage/models/v1"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

type Project struct {
	BaseModel
	Description string
	Users       []User `gorm:"constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
	Name        string
	Status      string
	Labels      []Label    `gorm:"many2many:project_labels;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
	APIToken    []APIToken `gorm:"constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
	Datasets    []Dataset  `gorm:"constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
}

func (project *Project) ToProtoModel(stats *v1storagemodels.ProjectStats) (*v1storagemodels.Project, error) {
	users := []*v1storagemodels.User{}

	for _, user := range project.Users {
		users = append(users, user.ToProtoModel())
	}

	labels := []*v1storagemodels.Label{}
	for _, label := range project.Labels {
		labels = append(labels, label.ToProtoModel())
	}

	status, err := ToStatus(project.Status)
	if err != nil {
		log.Errorln(err.Error())
		return nil, err
	}

	return &v1storagemodels.Project{
		Id:          project.ID.String(),
		Name:        project.Name,
		Description: project.Description,
		Users:       users,
		Labels:      labels,
		Status:      status,
		Stats:       stats,
	}, nil
}

type User struct {
	CreatedAt    time.Time
	UpdatedAt    time.Time
	DeletedAt    gorm.DeletedAt `gorm:"index"`
	UserOauth2ID string         `gorm:"primaryKey"`
	ProjectID    uuid.UUID      `gorm:"primaryKey;type:uuid"`
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
