package authz

import (
	v1storagemodels "github.com/ScienceObjectsDB/go-api/sciobjsdb/api/storage/models/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc/metadata"
)

type TestHandler struct {
}

func (projectHandler *TestHandler) GetUserID(metadata metadata.MD) (uuid.UUID, error) {
	return uuid.New(), nil
}

func (projectHandler *TestHandler) Authorize(projectID uuid.UUID, requestedRight v1storagemodels.Right, metadata metadata.MD) error {
	return nil
}

func (projectHandler *TestHandler) AuthorizeCreateProject(metadata metadata.MD) error {
	return nil
}

func (projectHandler *TestHandler) AuthorizeRead(metadata metadata.MD) error {
	return nil
}
