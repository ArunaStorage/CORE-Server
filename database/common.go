package database

import (
	"github.com/ScienceObjectsDB/CORE-Server/objectstorage"
	"gorm.io/gorm"
)

// Common Reusable struct for the database handler
// The database handlers are subdivided into CRUD operations
// Each operation usually gets the request and performs all required actions based on that request.
type Common struct {
	DB        *gorm.DB
	S3Handler *objectstorage.S3ObjectStorageHandler
}
