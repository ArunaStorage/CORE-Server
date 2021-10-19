package models

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

type StreamingEntry struct {
	ID           uuid.UUID `gorm:"type:uuid;default:uuid_generate_v4()"`
	CreatedAt    time.Time
	UpdatedAt    time.Time
	DeletedAt    gorm.DeletedAt `gorm:"index"`
	UUID         string         `gorm:"index"`
	Secret       string
	DatasetID    uuid.UUID
	Dataset      Dataset
	ProjectID    uuid.UUID
	Project      Project
	ObjectGroups []ObjectGroup `gorm:"many2many:streaming_entry_object_groups;"`
}
