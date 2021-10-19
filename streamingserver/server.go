package streamingserver

import (
	"fmt"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"

	"github.com/ScienceObjectsDB/CORE-Server/database"
	"github.com/ScienceObjectsDB/CORE-Server/models"
	"github.com/ScienceObjectsDB/CORE-Server/objectstorage"
	"github.com/ScienceObjectsDB/CORE-Server/signing"
	v1 "github.com/ScienceObjectsDB/go-api/api/services/v1"
	"github.com/gin-gonic/gin"
	"golang.org/x/sync/errgroup"
)

// DataStreamingServer Provides endpoint to stream a given set of objects via a presigned http get call
// The used link has to be created via the regular gRPC endpoints and can be used by anyone who has the link
type DataStreamingServer struct {
	SigningSecret string
	ReadHandler   *database.Read
	ObjectHandler *objectstorage.S3ObjectStorageHandler
}

// Starts the server on port 9011
func (server *DataStreamingServer) Run() error {
	r := gin.Default()
	r.GET("/dataset", server.datasetStream)

	return r.Run(":9011")
}

// Handles a stream that bundles all objectgroups of a dataset into a single byte stream
func (server *DataStreamingServer) datasetStream(c *gin.Context) {
	c.Request.URL.Host = c.Request.Host
	if c.Request.URL.Scheme == "" && c.Request.Host == "localhost" {
		c.Request.URL.Scheme = "http"
	}

	if c.Request.URL.Scheme == "" && c.Request.Host != "localhost" {
		c.Request.URL.Scheme = "https"
	}

	verified, err := signing.VerifyHMAC_sha256(server.SigningSecret, c.Request.URL)
	if err != nil {
		log.Println(err.Error())
		c.AbortWithStatus(503)
		return
	}

	if !verified {
		c.AbortWithStatus(403)
		return
	}

	datasetIDString := c.Query("id")
	datasetID := uuid.MustParse(datasetIDString)
	if err != nil {
		log.Println(err.Error())
		c.AbortWithError(400, fmt.Errorf("could not parse id value"))
		return
	}

	c.Status(200)
	c.Header("Content-Disposition", `attachment; filename="test.tar.gz"`)

	objectGroups, err := server.ReadHandler.GetDatasetObjectGroups(datasetID)
	if err != nil {
		log.Println(err.Error())
		c.AbortWithStatus(503)
		return
	}

	packer := ObjectsPacker{
		StreamType:    v1.GetObjectGroupsStreamLinkRequest_STREAM_TYPE_TARGZ,
		TargetWrite:   c.Writer,
		ObjectHandler: server.ObjectHandler,
	}

	objectGroupsChan := make(chan *models.ObjectGroup, 10)
	objectGroupsErrGrp := errgroup.Group{}
	objectGroupsErrGrp.Go(func() error {
		defer close(objectGroupsChan)
		for _, objectGroup := range objectGroups {
			objectGroupsChan <- objectGroup
		}

		return nil
	})

	err = packer.PackageObjects(objectGroupsChan)
	if err != nil {
		log.Println(err.Error())
		c.AbortWithStatus(503)
		return
	}
}
