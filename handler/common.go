package handler

import (
	"github.com/ScienceObjectsDB/CORE-Server/objectstorage"
	"gorm.io/gorm"
)

type Common struct {
	DB        *gorm.DB
	S3Handler *objectstorage.S3ObjectStorageHandler
}
