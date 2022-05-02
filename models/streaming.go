package models

import (
	"github.com/google/uuid"
)

type StreamingEntry struct {
	BaseModel
	UUID         string `gorm:"index"`
	Secret       string
	DatasetID    uuid.UUID
	Dataset      Dataset
	ProjectID    uuid.UUID
	Project      Project
	ObjectGroups []ObjectGroupRevision `gorm:"many2many:streaming_entry_object_groups;"`
}
