package models

import "gorm.io/gorm"

type StreamingEntry struct {
	gorm.Model
	UUID         string `gorm:"index"`
	Secret       string
	DatasetID    uint
	Dataset      Dataset
	ProjectID    uint
	Project      Project
	ObjectGroups []ObjectGroup `gorm:"many2many:streaming_entry_object_groups;"`
}
