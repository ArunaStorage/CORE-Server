package authz

import (
	protoModels "github.com/ScienceObjectsDB/go-api/api/models/v1"
	"google.golang.org/grpc/metadata"
)

type AuthInterface interface {
	GetUserID(metadata metadata.MD) (string, error)
	Authorize(projectID uint, requestedRight protoModels.Right, metadata metadata.MD) error
}
