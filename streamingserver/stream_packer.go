package streamingserver

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"net/http"

	log "github.com/sirupsen/logrus"

	"github.com/ScienceObjectsDB/CORE-Server/models"
	"github.com/ScienceObjectsDB/CORE-Server/objectstorage"
	services "github.com/ScienceObjectsDB/go-api/api/services/v1"
	"golang.org/x/sync/errgroup"
)

// ObjectsPacker packages a stream of stored objects into a single download link
// This can be used to e.g. provide data to a cooperation partner
// The provided link is secured using hmac
type ObjectsPacker struct {
	StreamType    services.GetObjectGroupsStreamLinkRequest_StreamType
	TargetWrite   FlushingWriter
	ObjectHandler *objectstorage.S3ObjectStorageHandler
}

// FlushingWriter Interface to represent a flushable writer
type FlushingWriter interface {
	io.Writer
	http.Flusher
}

// PackageObjects takes all objects from the provided channel and packages them into a single bytes stream
// and writes the stream into the provided TargetWrite writer
// Packaging details depend on the configuration of the ObjectsPacker interface
func (packer *ObjectsPacker) PackageObjects(objectGroups chan *models.ObjectGroup) error {
	switch packer.StreamType {
	case services.GetObjectGroupsStreamLinkRequest_STREAM_TYPE_TARGZ:
		return packer.handleTarGZStream(objectGroups)
	default:
		{
			return fmt.Errorf("could not handle requested data stream type")
		}
	}
}

// Packer implementation to bundle objects into a tar archive and compress the resulting bytestream with gunzip
// The data is written and read in chunks
func (packer *ObjectsPacker) handleTarGZStream(objectGroups chan *models.ObjectGroup) error {
	gunzipWriter := gzip.NewWriter(packer.TargetWrite)
	tarWriter := tar.NewWriter(gunzipWriter)

	for objectGroup := range objectGroups {
		groupName := objectGroup.Name
		err := tarWriter.WriteHeader(&tar.Header{
			Name:    fmt.Sprintf("%v/", groupName),
			ModTime: objectGroup.UpdatedAt,
		})
		if err != nil {
			log.Println(err.Error())
			return err
		}
		for _, object := range objectGroup.Objects {
			err = tarWriter.WriteHeader(&tar.Header{
				Name:    fmt.Sprintf("%v/%v", objectGroup.Name, object.Filename),
				ModTime: object.UpdatedAt,
				Mode:    0700,
				Size:    object.ContentLen,
			})
			if err != nil {
				log.Println(err.Error())
				return err
			}

			chunkChannel := make(chan []byte, 10)
			chunkedLoaderWaitGrop := errgroup.Group{}
			chunkedLoaderWaitGrop.Go(func() error {
				err := packer.ObjectHandler.ChunkedObjectDowload(&object, chunkChannel)
				if err != nil {
					log.Println(err.Error())
					return err
				}

				close(chunkChannel)
				return nil
			})

			err := packer.writeObjectsData(chunkChannel, tarWriter)
			if err != nil {
				log.Println(err.Error())
				return err
			}
		}
	}

	err := tarWriter.Close()
	if err != nil {
		log.Println(err.Error())
		return err
	}

	err = gunzipWriter.Close()
	if err != nil {
		log.Println(err.Error())
		return err
	}

	return nil
}

// Writes the provided data buffer into the provided writter
func (packer *ObjectsPacker) writeObjectsData(data chan []byte, writer io.Writer) error {
	for chunk := range data {
		_, err := writer.Write(chunk)
		if err != nil {
			log.Println(err.Error())
			return err
		}
		packer.TargetWrite.Flush()
	}

	return nil
}
