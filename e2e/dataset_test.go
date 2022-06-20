package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	log "github.com/sirupsen/logrus"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	v1storagemodels "github.com/ScienceObjectsDB/go-api/sciobjsdb/api/storage/models/v1"
	v1storageservices "github.com/ScienceObjectsDB/go-api/sciobjsdb/api/storage/services/v1"
)

type TestMetadata struct {
	Testdata1 string
	Testdata2 int
}

func TestDataset(t *testing.T) {
	createProjectRequest := &v1storageservices.CreateProjectRequest{
		Name:        "testproject_dataset",
		Description: "test",
	}

	createResponse, err := ServerEndpoints.project.CreateProject(context.Background(), createProjectRequest)
	if err != nil {
		log.Fatalln(err.Error())
	}

	datasetLabel := []*v1storagemodels.Label{
		{
			Key:   "Label1",
			Value: "LabelValue1",
		},
		{
			Key:   "Label2",
			Value: "LabelValue2",
		},
	}

	metadataEntries := []*v1storageservices.CreateObjectRequest{
		{
			Filename: "metadata1.json",
			Filetype: "json",
			Labels: []*v1storagemodels.Label{
				{Key: "label1key", Value: "label1value"},
			},
		},
		{
			Filename: "metadata2.json",
			Filetype: "json",
			Labels: []*v1storagemodels.Label{
				{Key: "label1key", Value: "label1value"},
			},
		},
	}

	createDatasetRequest := &v1storageservices.CreateDatasetRequest{
		Name:            "testdataset",
		ProjectId:       createResponse.GetId(),
		Labels:          datasetLabel,
		MetadataObjects: metadataEntries,
	}

	datasetCreateResponse, err := ServerEndpoints.dataset.CreateDataset(context.Background(), createDatasetRequest)
	if err != nil {
		log.Fatalln(err.Error())
	}

	testmetadata := TestMetadata{
		Testdata1: "foo",
		Testdata2: 15,
	}

	metadatabytes, err := json.Marshal(testmetadata)
	if err != nil {
		log.Fatalln(err.Error())
	}

	datasetGetResponse, err := ServerEndpoints.dataset.GetDataset(context.Background(), &v1storageservices.GetDatasetRequest{
		Id: datasetCreateResponse.GetId(),
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	for _, object := range datasetGetResponse.GetDataset().MetadataObjects {
		linkResponse, err := ServerEndpoints.load.CreateUploadLink(context.Background(), &v1storageservices.CreateUploadLinkRequest{
			Id: object.GetId(),
		})
		if err != nil {
			log.Fatalln(err.Error())
		}

		req, err := http.NewRequest("PUT", linkResponse.UploadLink, bytes.NewBuffer(metadatabytes))
		if err != nil {
			log.Fatalln(err.Error())
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			log.Fatalln(err.Error())
		}

		if resp.StatusCode != http.StatusOK {
			log.Fatalln("error when uploading data")
		}
	}

	datasetGetResponseWithMetadata, err := ServerEndpoints.dataset.GetDataset(context.Background(), &v1storageservices.GetDatasetRequest{
		Id: datasetCreateResponse.GetId(),
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	assert.Equal(t, 2, len(datasetGetResponseWithMetadata.Dataset.MetadataObjects))

	assert.Equal(t, createDatasetRequest.Name, datasetGetResponse.Dataset.Name)
	assert.Equal(t, createDatasetRequest.Description, datasetGetResponse.GetDataset().Description)
	assert.ElementsMatch(t, createDatasetRequest.Labels, datasetGetResponse.Dataset.Labels)

	//_, err = ServerEndpoints.dataset.DeleteDataset(context.Background(), &services.DeleteDatasetRequest{
	//	Id: datasetCreateResponse.GetId(),
	//})
	//if err != nil {
	//	log.Fatalln(err.Error())
	//
	//}

}

func TestDatasetObjects(t *testing.T) {
	createProjectRequest := &v1storageservices.CreateProjectRequest{
		Name:        "testproject_dataset",
		Description: "test",
	}

	createResponse, err := ServerEndpoints.project.CreateProject(context.Background(), createProjectRequest)
	if err != nil {
		log.Fatalln(err.Error())
	}

	createDatasetRequest := &v1storageservices.CreateDatasetRequest{
		Name:      "testdataset",
		ProjectId: createResponse.GetId(),
	}

	datasetCreateResponse, err := ServerEndpoints.dataset.CreateDataset(context.Background(), createDatasetRequest)
	if err != nil {
		log.Fatalln(err.Error())
	}

	fwLabel := v1storagemodels.Label{
		Key:   "genomic.bioinformatics/read_orientation",
		Value: "forward",
	}

	revLabel := v1storagemodels.Label{
		Key:   "genomic.bioinformatics/read_orientation",
		Value: "reverse",
	}

	experimentLabel := v1storagemodels.Label{
		Key:   "genomic.bioinformatics/experiment_id",
		Value: "id_1234",
	}

	fwReadObjectResponse, err := ServerEndpoints.object.CreateObject(context.Background(), &v1storageservices.CreateObjectRequest{
		Filename:  "forward.fasta",
		Filetype:  "fasta",
		DatasetId: datasetCreateResponse.GetId(),
		Labels: []*v1storagemodels.Label{
			&fwLabel, &experimentLabel,
		},
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	revReadObjectResponse, err := ServerEndpoints.object.CreateObject(context.Background(), &v1storageservices.CreateObjectRequest{
		Filename:  "reverse.fasta",
		Filetype:  "fasta",
		DatasetId: datasetCreateResponse.GetId(),
		Labels: []*v1storagemodels.Label{
			&revLabel, &experimentLabel,
		},
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	_, err = ServerEndpoints.object.FinishObjectUpload(context.Background(), &v1storageservices.FinishObjectUploadRequest{
		Id: fwReadObjectResponse.GetId(),
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	_, err = ServerEndpoints.object.FinishObjectUpload(context.Background(), &v1storageservices.FinishObjectUploadRequest{
		Id: revReadObjectResponse.GetId(),
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	objects, err := ServerEndpoints.dataset.GetDatasetObjects(context.Background(), &v1storageservices.GetDatasetObjectsRequest{
		Id: datasetCreateResponse.GetId(),
		LabelFilter: &v1storagemodels.LabelFilter{
			Labels: []*v1storagemodels.Label{
				&fwLabel,
			},
		},
	})

	if err != nil {
		log.Fatalln(err.Error())
	}

	assert.Equal(t, 1, len(objects.GetObjects()))

	experimentObjects, err := ServerEndpoints.dataset.GetDatasetObjects(context.Background(), &v1storageservices.GetDatasetObjectsRequest{
		Id: datasetCreateResponse.GetId(),
		LabelFilter: &v1storagemodels.LabelFilter{
			Labels: []*v1storagemodels.Label{
				&experimentLabel,
			},
		},
	})

	if err != nil {
		log.Fatalln(err.Error())
	}

	assert.Equal(t, 2, len(experimentObjects.GetObjects()))

}

func TestDatasetObjectGroupsPagination(t *testing.T) {
	createProjectRequest := &v1storageservices.CreateProjectRequest{
		Name:        "testproject_dataset",
		Description: "test",
	}

	createResponse, err := ServerEndpoints.project.CreateProject(context.Background(), createProjectRequest)
	if err != nil {
		log.Fatalln(err.Error())
	}

	createDatasetRequest := &v1storageservices.CreateDatasetRequest{
		Name:      "testdataset",
		ProjectId: createResponse.GetId(),
	}

	datasetCreateResponse, err := ServerEndpoints.dataset.CreateDataset(context.Background(), createDatasetRequest)
	if err != nil {
		log.Fatalln(err.Error())
	}

	for i := 0; i < 10; i++ {
		createObjectGroup := &v1storageservices.CreateObjectGroupRequest{
			CreateRevisionRequest: &v1storageservices.CreateObjectGroupRevisionRequest{
				Name:              fmt.Sprintf("foobar-%v", i),
				Description:       "foo",
				UpdateObjects:     &v1storageservices.UpdateObjectsRequests{},
				UpdateMetaObjects: &v1storageservices.UpdateObjectsRequests{},
			},
			DatasetId: datasetCreateResponse.GetId(),
		}

		_, err := ServerEndpoints.object.CreateObjectGroup(context.Background(), createObjectGroup)
		if err != nil {
			log.Fatalln(err.Error())
		}
	}

	handledObjectGroups := make(map[string]struct{})

	objectGroups1, err := ServerEndpoints.dataset.ReadHandler.GetDatasetObjectGroups(uuid.MustParse(datasetCreateResponse.GetId()), &v1storagemodels.PageRequest{
		LastUuid: "",
		PageSize: 4,
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	assert.Equal(t, 4, len(objectGroups1))

	var lastUUID uuid.UUID

	for _, objectGroup := range objectGroups1 {
		if _, ok := handledObjectGroups[objectGroup.CurrentObjectGroupRevision.Name]; !ok {
			handledObjectGroups[objectGroup.CurrentObjectGroupRevision.Name] = struct{}{}
			lastUUID = objectGroup.ID
		} else {
			log.Fatalln("found duplicate object group in pagination")
		}
	}

	objectGroups2, err := ServerEndpoints.dataset.ReadHandler.GetDatasetObjectGroups(uuid.MustParse(datasetCreateResponse.GetId()), &v1storagemodels.PageRequest{
		LastUuid: lastUUID.String(),
		PageSize: 4,
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	assert.Equal(t, 4, len(objectGroups2))

	for _, objectGroup := range objectGroups2 {
		if _, ok := handledObjectGroups[objectGroup.CurrentObjectGroupRevision.Name]; !ok {
			handledObjectGroups[objectGroup.CurrentObjectGroupRevision.Name] = struct{}{}
			lastUUID = objectGroup.ID
		} else {
			log.Fatalln("found duplicate object group in pagination")
		}
	}

	objectGroups3, err := ServerEndpoints.dataset.ReadHandler.GetDatasetObjectGroups(uuid.MustParse(datasetCreateResponse.GetId()), &v1storagemodels.PageRequest{
		LastUuid: lastUUID.String(),
		PageSize: 2,
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	assert.Equal(t, 2, len(objectGroups3))

	for _, objectGroup := range objectGroups3 {
		if _, ok := handledObjectGroups[objectGroup.CurrentObjectGroupRevision.Name]; !ok {
			handledObjectGroups[objectGroup.CurrentObjectGroupRevision.Name] = struct{}{}
		} else {
			log.Fatalln("found duplicate object group in pagination")
		}
	}
}
