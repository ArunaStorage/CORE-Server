package models

import "github.com/google/uuid"

type StreamGroup struct {
	BaseModel
	Subject        string
	ResourceID     uuid.UUID `gorm:"index"`
	ResourceType   string    `gorm:"index"`
	UseSubResource bool
	ProjectID      uuid.UUID `gorm:"index"`
	Project        Project
}
