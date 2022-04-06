package e2e

import (
	"bytes"
	"context"
	"io/ioutil"
	"log"
	"net/http"
	"testing"

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

func UploadObjects(objects []*v1resourcemodels.Object, data []string, loadendpoint *server.LoadEndpoints, objectEndpoint *server.ObjectServerEndpoints) error {
	for i, object := range objects {
		objectData := data[i]
		objectUploadLink, err := loadendpoint.CreateUploadLink(context.Background(), &v1storagemodels.CreateUploadLinkRequest{
			Id: object.Id,
		})

		if err != nil {
			log.Println(err.Error())
			return err
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
			Id: object.Id,
		})
		if err != nil {
			log.Fatalln(err.Error())
		}
	}

	return nil
}
