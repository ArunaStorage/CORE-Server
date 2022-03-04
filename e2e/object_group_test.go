package e2e

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"

	v1storagemodels "github.com/ScienceObjectsDB/go-api/sciobjsdb/api/storage/models/v1"
	v1storageservices "github.com/ScienceObjectsDB/go-api/sciobjsdb/api/storage/services/v1"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestObjectGroup(t *testing.T) {
	createProjectRequest := &v1storageservices.CreateProjectRequest{
		Name:        "testproject_dataset",
		Description: "test",
		Metadata: []*v1storagemodels.Metadata{
			{
				Key:      "TestKey1",
				Metadata: []byte("mymetadata1"),
			},
			{
				Key:      "TestKey2",
				Metadata: []byte("mymetadata2"),
			},
		},
	}

	createResponse, err := ServerEndpoints.project.CreateProject(context.Background(), createProjectRequest)
	if err != nil {
		log.Fatalln(err.Error())
	}

	datasetMetadata := []*v1storagemodels.Metadata{
		{
			Key:      "Key1",
			Metadata: []byte("dasddasd"),
		},
		{
			Key:      "Key2",
			Metadata: []byte("asdasd"),
		},
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

	createDatasetRequest := &v1storageservices.CreateDatasetRequest{
		Name:      "testdataset",
		ProjectId: createResponse.GetId(),
		Metadata:  datasetMetadata,
		Labels:    datasetLabel,
	}

	datasetCreateResponse, err := ServerEndpoints.dataset.CreateDataset(context.Background(), createDatasetRequest)
	if err != nil {
		log.Fatalln(err.Error())
	}

	objectGroupMetadata := []*v1storagemodels.Metadata{
		{
			Key:      "Key1OG",
			Metadata: []byte("dasddasdOG"),
		},
		{
			Key:      "Key2OG",
			Metadata: []byte("asdasdOG"),
		},
	}

	objectGroupLabel := []*v1storagemodels.Label{
		{
			Key:   "Label1OG",
			Value: "LabelValue1OG",
		},
		{
			Key:   "Label2OG",
			Value: "LabelValue2OG",
		},
	}

	object1Metadata := []*v1storagemodels.Metadata{
		{
			Key:      "Key1O1",
			Metadata: []byte("dasddasdO1"),
		},
		{
			Key:      "Key2OG1",
			Metadata: []byte("asdasdO1"),
		},
	}

	object1Label := []*v1storagemodels.Label{
		{
			Key:   "Label1O1",
			Value: "LabelValue1O1",
		},
		{
			Key:   "Label2O1",
			Value: "LabelValue2O1",
		},
	}

	object2Metadata := []*v1storagemodels.Metadata{
		{
			Key:      "Key1O2",
			Metadata: []byte("dasddasdO2"),
		},
		{
			Key:      "Key2O2",
			Metadata: []byte("asdasdO2"),
		},
	}

	object2Label := []*v1storagemodels.Label{
		{
			Key:   "Label1O2",
			Value: "LabelValue1O2",
		},
		{
			Key:   "Label2O2",
			Value: "LabelValue2O2",
		},
	}

	name := fmt.Sprintf("objectgroup-%v", uuid.New())

	createObjectGroupRequest := &v1storageservices.CreateObjectGroupRequest{
		Name:      name,
		DatasetId: datasetCreateResponse.GetId(),
		Labels:    objectGroupLabel,
		Metadata:  objectGroupMetadata,
		Objects: []*v1storageservices.CreateObjectRequest{
			{
				Filename:   "testfile1",
				Filetype:   "bin",
				Labels:     object1Label,
				Metadata:   object1Metadata,
				ContentLen: 3,
			},
			{
				Filename:   "testfile2",
				Filetype:   "bin",
				Labels:     object2Label,
				Metadata:   object2Metadata,
				ContentLen: 3,
			},
		},
	}

	createObjectGroupResponse, err := ServerEndpoints.object.CreateObjectGroup(context.Background(), createObjectGroupRequest)
	if err != nil {
		log.Fatalln(err.Error())
	}

	assert.NotEqual(t, createObjectGroupResponse.ObjectGroupId, 0)

	getObjectGroupResponse, err := ServerEndpoints.object.GetObjectGroup(context.Background(), &v1storageservices.GetObjectGroupRequest{
		Id: createObjectGroupResponse.ObjectGroupId,
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	assert.Equal(t, createObjectGroupRequest.Name, getObjectGroupResponse.ObjectGroup.Name)
	assert.Equal(t, createObjectGroupRequest.DatasetId, getObjectGroupResponse.ObjectGroup.DatasetId)
	assert.Equal(t, createDatasetRequest.Description, getObjectGroupResponse.GetObjectGroup().Description)
	assert.ElementsMatch(t, createObjectGroupRequest.Labels, getObjectGroupResponse.ObjectGroup.Labels)
	assert.ElementsMatch(t, createObjectGroupRequest.Metadata, getObjectGroupResponse.ObjectGroup.Metadata)

	assert.Equal(t, "testfile1", getObjectGroupResponse.ObjectGroup.Objects[0].Filename)

	object := getObjectGroupResponse.ObjectGroup.Objects[0]

	uploadLink, err := ServerEndpoints.load.CreateUploadLink(context.Background(), &v1storageservices.CreateUploadLinkRequest{
		Id: object.GetId(),
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	uploadHttpRequest, err := http.NewRequest("PUT", uploadLink.UploadLink, bytes.NewBufferString("foo"))
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

	_, err = ServerEndpoints.object.FinishObjectUpload(context.Background(), &v1storageservices.FinishObjectUploadRequest{
		Id: object.Id,
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	_, err = ServerEndpoints.object.FinishObjectGroupUpload(context.Background(), &v1storageservices.FinishObjectGroupUploadRequest{
		Id: object.ObjectGroupId,
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	downloadLink, err := ServerEndpoints.load.CreateDownloadLink(context.Background(), &v1storageservices.CreateDownloadLinkRequest{
		Id: object.GetId(),
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	dlResponse, err := http.DefaultClient.Get(downloadLink.GetDownloadLink())
	if err != nil {
		log.Fatalln(err.Error())
	}

	if response.StatusCode != 200 {
		log.Fatalln(response.Status)
	}

	data, err := ioutil.ReadAll(dlResponse.Body)
	if err != nil {
		log.Fatalln(err.Error())
	}

	assert.Equal(t, string(data), "foo")

	downloadLinkRange, err := ServerEndpoints.load.CreateDownloadLink(context.Background(), &v1storageservices.CreateDownloadLinkRequest{
		Id: object.GetId(),
		Range: &v1storageservices.CreateDownloadLinkRequest_Range{
			StartByte: 0,
			EndByte:   1,
		},
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	rangeDLRequest, err := http.NewRequest("GET", downloadLinkRange.GetDownloadLink(), &bytes.Reader{})
	if err != nil {
		log.Fatalln(err.Error())
	}

	rangeDLRequest.Header.Add("Range", "bytes=0-1")

	dlResponseRange, err := http.DefaultClient.Do(rangeDLRequest)
	if err != nil {
		log.Fatalln(err.Error())
	}

	if response.StatusCode != 200 {
		log.Fatalln(response.Status)
	}

	dataRange, err := ioutil.ReadAll(dlResponseRange.Body)
	if err != nil {
		log.Fatalln(err.Error())
	}

	assert.Equal(t, string(dataRange), "fo")

}

func TestObjectGroupBatch(t *testing.T) {
	projectID, err := ServerEndpoints.project.CreateProject(context.Background(), &v1storageservices.CreateProjectRequest{
		Name: "foo",
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	datasetID, err := ServerEndpoints.dataset.CreateDataset(context.Background(), &v1storageservices.CreateDatasetRequest{
		Name:      "foo",
		ProjectId: projectID.GetId(),
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	var requests []*v1storageservices.CreateObjectGroupRequest

	for i := 0; i < 10; i++ {
		createObjectGroupRequest := &v1storageservices.CreateObjectGroupRequest{
			Name:      fmt.Sprintf("foo-%v", i),
			DatasetId: datasetID.GetId(),
			Objects: []*v1storageservices.CreateObjectRequest{
				{
					Filename: "ff.bin",
				},
				{
					Filename: "fu.bin",
				},
			},
		}
		requests = append(requests, createObjectGroupRequest)
	}

	result, err := ServerEndpoints.object.CreateObjectGroupBatch(context.Background(), &v1storageservices.CreateObjectGroupBatchRequest{
		Requests:          requests,
		IncludeObjectLink: true,
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	if len(result.Responses) != len(requests) {
		t.Fatalf("wrong number of result found")
	}

	for _, objectgroup := range result.GetResponses() {
		if len(objectgroup.ObjectLinks) != 2 {
			log.Fatalln("wrong number of upload links found")
		}
		for _, object := range objectgroup.ObjectLinks {
			uploadHttpRequest, err := http.NewRequest("PUT", object.Link, bytes.NewBufferString("foo"))
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
		}
	}
}

func TestObjectGroupsDates(t *testing.T) {
	projectID, err := ServerEndpoints.project.CreateProject(context.Background(), &v1storageservices.CreateProjectRequest{
		Name: "foo",
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	datasetID, err := ServerEndpoints.dataset.CreateDataset(context.Background(), &v1storageservices.CreateDatasetRequest{
		Name:      "foo",
		ProjectId: projectID.GetId(),
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	objectGroupTooEarly1 := v1storageservices.CreateObjectGroupRequest{
		Name:      "early1",
		DatasetId: datasetID.GetId(),
		Generated: timestamppb.New(time.Date(1990, time.July, 27, 0, 0, 0, 0, time.Local)),
	}

	project, err := ServerEndpoints.project.GetProject(context.Background(), &v1storageservices.GetProjectRequest{
		Id: projectID.GetId(),
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	_, err = ServerEndpoints.dataset.CreateHandler.CreateObjectGroup(&objectGroupTooEarly1, project.Project.Bucket)
	if err != nil {
		log.Fatalln(err.Error())
	}

	objectGroupTooEarly2 := v1storageservices.CreateObjectGroupRequest{
		Name:      "early2",
		DatasetId: datasetID.GetId(),
		Generated: timestamppb.New(time.Date(1992, time.July, 27, 0, 0, 0, 0, time.Local)),
	}

	_, err = ServerEndpoints.dataset.CreateHandler.CreateObjectGroup(&objectGroupTooEarly2, project.Project.Bucket)
	if err != nil {
		log.Fatalln(err.Error())
	}

	objectGroupInTime1 := v1storageservices.CreateObjectGroupRequest{
		Name:      "intime1",
		DatasetId: datasetID.GetId(),
		Generated: timestamppb.New(time.Date(2000, time.July, 27, 0, 0, 0, 0, time.Local)),
	}

	_, err = ServerEndpoints.dataset.CreateHandler.CreateObjectGroup(&objectGroupInTime1, project.Project.Bucket)
	if err != nil {
		log.Fatalln(err.Error())
	}

	objectGroupInTime2 := v1storageservices.CreateObjectGroupRequest{
		Name:      "intime2",
		DatasetId: datasetID.GetId(),
		Generated: timestamppb.New(time.Date(2000, time.December, 27, 0, 0, 0, 0, time.Local)),
	}

	_, err = ServerEndpoints.dataset.CreateHandler.CreateObjectGroup(&objectGroupInTime2, project.Project.Bucket)
	if err != nil {
		log.Fatalln(err.Error())
	}

	objectGroupTooLate1 := v1storageservices.CreateObjectGroupRequest{
		Name:      "late1",
		DatasetId: datasetID.GetId(),
		Generated: timestamppb.Now(),
	}

	_, err = ServerEndpoints.dataset.CreateHandler.CreateObjectGroup(&objectGroupTooLate1, project.Project.Bucket)
	if err != nil {
		log.Fatalln(err.Error())
	}

	objectGroupTooLate2 := v1storageservices.CreateObjectGroupRequest{
		Name:      "late2",
		DatasetId: datasetID.GetId(),
		Generated: timestamppb.Now(),
	}

	_, err = ServerEndpoints.dataset.CreateHandler.CreateObjectGroup(&objectGroupTooLate2, project.Project.Bucket)
	if err != nil {
		log.Fatalln(err.Error())
	}

	objectGroups, err := ServerEndpoints.dataset.ReadHandler.GetObjectGroupsInDateRange(
		uuid.MustParse(datasetID.GetId()),
		time.Date(1995, time.December, 27, 0, 0, 0, 0, time.Local),
		time.Date(2015, time.December, 27, 0, 0, 0, 0, time.Local))
	if err != nil {
		log.Fatalln(err.Error())
	}

	assert.Equal(t, len(objectGroups), 2)
}

func TestObjectGroupDuplicates(t *testing.T) {
	projectID, err := ServerEndpoints.project.CreateProject(context.Background(), &v1storageservices.CreateProjectRequest{
		Name: "foo",
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	datasetID1, err := ServerEndpoints.dataset.CreateDataset(context.Background(), &v1storageservices.CreateDatasetRequest{
		Name:      "foo-1",
		ProjectId: projectID.GetId(),
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	datasetID2, err := ServerEndpoints.dataset.CreateDataset(context.Background(), &v1storageservices.CreateDatasetRequest{
		Name:      "foo-2",
		ProjectId: projectID.GetId(),
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	_, err = ServerEndpoints.object.CreateObjectGroup(context.Background(), &v1storageservices.CreateObjectGroupRequest{
		Name:      "test-1",
		DatasetId: datasetID1.GetId(),
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	_, err = ServerEndpoints.object.CreateObjectGroup(context.Background(), &v1storageservices.CreateObjectGroupRequest{
		Name:      "test-2",
		DatasetId: datasetID1.GetId(),
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	_, err = ServerEndpoints.object.CreateObjectGroup(context.Background(), &v1storageservices.CreateObjectGroupRequest{
		Name:      "test-1",
		DatasetId: datasetID1.GetId(),
	})
	assert.Error(t, err)

	_, err = ServerEndpoints.object.CreateObjectGroup(context.Background(), &v1storageservices.CreateObjectGroupRequest{
		Name:      "test-1",
		DatasetId: datasetID2.GetId(),
	})
	if err != nil {
		log.Fatalln(err.Error())
	}
}
