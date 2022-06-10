package e2e

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"testing"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"

	"github.com/ScienceObjectsDB/CORE-Server/models"
	"github.com/ScienceObjectsDB/CORE-Server/server"
	v1resourcemodels "github.com/ScienceObjectsDB/go-api/sciobjsdb/api/storage/models/v1"
	v1storagemodels "github.com/ScienceObjectsDB/go-api/sciobjsdb/api/storage/services/v1"
	"github.com/stretchr/testify/assert"
)

func DownloadObjects(t *testing.T, objects []*v1resourcemodels.Object, data []string, loadendpoint *server.LoadEndpoints, objectEndpoint *server.ObjectServerEndpoints) error {
	for i, object := range objects {
		expectedObjectData := data[i]

		link, err := loadendpoint.CreateDownloadLink(context.Background(), &v1storagemodels.CreateDownloadLinkRequest{
			Id: object.Id,
		})

		if err != nil {
			log.Println(err.Error())
			return err
		}

		resp, err := http.DefaultClient.Get(link.GetDownloadLink())

		if resp.StatusCode != 200 {
			log.Fatalln(resp.Status)
		}

		if err != nil {
			log.Println(err.Error())
			return err
		}

		actualObjectData, err := ioutil.ReadAll(resp.Body)

		if err != nil {
			log.Println(err.Error())
			return err
		}

		actualObjectDataString := string(actualObjectData)

		assert.Equal(t, expectedObjectData, actualObjectDataString)
	}

	return nil
}

func UploadObjects(loadendpoint *server.LoadEndpoints, objectEndpoint *server.ObjectServerEndpoints, objectNumber int, datasetID string, prefix string) ([]*models.Object, error) {
	var modelsObjects []*models.Object

	for i := 0; i <= objectNumber; i++ {
		objectData := fmt.Sprintf("%v-data-%v", prefix, i)
		object := &v1storagemodels.CreateObjectRequest{
			Filename:   fmt.Sprintf("%v-file%v.bin", prefix, i),
			Filetype:   "bin",
			ContentLen: int64(len(objectData)),
			DatasetId:  datasetID,
			Labels: []*v1resourcemodels.Label{
				&v1resourcemodels.Label{
					Key:   fmt.Sprintf("key-%v-1", i),
					Value: fmt.Sprintf("value-%v-1", i),
				},
				&v1resourcemodels.Label{
					Key:   fmt.Sprintf("key-%v-2", i),
					Value: fmt.Sprintf("value-%v-2", i),
				},
				&v1resourcemodels.Label{
					Key:   fmt.Sprintf("key-%v-3", i),
					Value: fmt.Sprintf("value-%v-3", i),
				},
			},
		}

		createObjectResponse, err := objectEndpoint.CreateObject(context.Background(), object)
		if err != nil {
			log.Errorln(err.Error())
			return nil, err
		}

		modelLabels := make([]models.Label, len(object.Labels))
		for i, label := range object.Labels {
			modelLabel := models.Label{
				Key:   label.Key,
				Value: label.Value,
			}

			modelLabels[i] = modelLabel
		}

		modelObject := &models.Object{
			Filename:   object.Filename,
			Filetype:   object.Filetype,
			ContentLen: object.GetContentLen(),
			Status:     v1resourcemodels.Status_STATUS_AVAILABLE.String(),
			Labels:     modelLabels,
		}
		objectID := uuid.MustParse(createObjectResponse.GetId())

		modelObject.ID = objectID

		objectUploadLink, err := loadendpoint.CreateUploadLink(context.Background(), &v1storagemodels.CreateUploadLinkRequest{
			Id: objectID.String(),
		})

		if err != nil {
			log.Errorln(err.Error())
			return nil, err
		}

		uploadHttpRequest, err := http.NewRequest("PUT", objectUploadLink.UploadLink, bytes.NewBufferString(objectData))
		if err != nil {
			log.Fatalln(err.Error())
		}

		response, err := http.DefaultClient.Do(uploadHttpRequest)
		if err != nil {
			log.Fatalln(err.Error())
		}

		if response.StatusCode != 200 {
			log.Fatalln(response.Status)
		}

		_, err = objectEndpoint.FinishObjectUpload(context.Background(), &v1storagemodels.FinishObjectUploadRequest{
			Id: objectID.String(),
		})
		if err != nil {
			log.Errorln(err.Error())
			return nil, err
		}

		modelsObjects = append(modelsObjects, modelObject)

	}

	return modelsObjects, nil
}
