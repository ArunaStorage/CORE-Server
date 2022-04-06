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

	createDatasetRequest := &v1storageservices.CreateDatasetRequest{
		Name:      "testdataset",
		ProjectId: createResponse.GetId(),
		Labels:    datasetLabel,
	}

	datasetCreateResponse, err := ServerEndpoints.dataset.CreateDataset(context.Background(), createDatasetRequest)
	if err != nil {
		log.Fatalln(err.Error())
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
		MetadataObjects: []*v1storageservices.CreateObjectRequest{
			{
				Filename:   "metadata1",
				Filetype:   "meta",
				ContentLen: 8,
			},
		},
		Objects: []*v1storageservices.CreateObjectRequest{
			{
				Filename:   "testfile1",
				Filetype:   "bin",
				Labels:     object1Label,
				ContentLen: 3,
			},
			{
				Filename:   "testfile2",
				Filetype:   "bin",
				Labels:     object2Label,
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

	assert.Equal(t, "testfile1", getObjectGroupResponse.ObjectGroup.Objects[0].Filename)

	object := getObjectGroupResponse.ObjectGroup.Objects[0]

	err = UploadObjects(getObjectGroupResponse.ObjectGroup.GetObjects(), []string{"foo", "baa"}, ServerEndpoints.load, ServerEndpoints.object)
	if err != nil {
		log.Fatalln(err.Error())
	}

	err = UploadObjects(getObjectGroupResponse.ObjectGroup.GetMetadataObjects(), []string{"metadata"}, ServerEndpoints.load, ServerEndpoints.object)
	if err != nil {
		log.Fatalln(err.Error())
	}

	_, err = ServerEndpoints.object.FinishObjectGroupUpload(context.Background(), &v1storageservices.FinishObjectGroupUploadRequest{
		Id: object.ObjectGroupId,
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	err = DownloadObjects(t, getObjectGroupResponse.ObjectGroup.Objects, []string{"foo", "baa"}, ServerEndpoints.load, ServerEndpoints.object)
	if err != nil {
		log.Fatalln(err.Error())
	}

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

	if dlResponseRange.StatusCode != 206 {
		log.Fatalln(dlResponseRange.Status)
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
					Filename:   "ff.bin",
					ContentLen: 3,
				},
				{
					Filename:   "fu.bin",
					ContentLen: 3,
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

	project, err := ServerEndpoints.project.GetProject(context.Background(), &v1storageservices.GetProjectRequest{
		Id: projectID.GetId(),
	})

	if err != nil {
		log.Fatalln(err.Error())
	}

	assert.Equal(t, int64(20), project.GetProject().GetStats().ObjectCount)
	assert.Equal(t, int64(10), project.GetProject().GetStats().ObjectGroupCount)
	assert.Equal(t, float64(3), project.GetProject().GetStats().AvgObjectSize)
	assert.Equal(t, int64(60), project.GetProject().GetStats().GetAccSize())
	assert.Equal(t, int64(1), project.GetProject().GetStats().GetUserCount())

	dataset, err := ServerEndpoints.dataset.GetDataset(context.Background(), &v1storageservices.GetDatasetRequest{
		Id: datasetID.GetId(),
	})

	if err != nil {
		log.Fatalln(err.Error())
	}

	assert.Equal(t, int64(20), dataset.GetDataset().GetStats().ObjectCount)
	assert.Equal(t, int64(10), dataset.GetDataset().GetStats().ObjectGroupCount)
	assert.Equal(t, float64(3), dataset.GetDataset().GetStats().AvgObjectSize)
	assert.Equal(t, int64(60), dataset.GetDataset().GetStats().GetAccSize())

	datasetobjectGroups, err := ServerEndpoints.dataset.GetDatasetObjectGroups(context.Background(), &v1storageservices.GetDatasetObjectGroupsRequest{
		Id: datasetID.GetId(),
	})

	if err != nil {
		log.Fatalln(err.Error())
	}

	for _, objectGroup := range datasetobjectGroups.ObjectGroups {
		objectGroupStats := objectGroup.GetStats()
		assert.Equal(t, int64(2), objectGroupStats.GetObjectCount())
		assert.Equal(t, float64(3), objectGroupStats.GetAvgObjectSize())
		assert.Equal(t, int64(6), objectGroupStats.GetAccSize())
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
		Name:      uuid.New().String(),
		DatasetId: datasetID1.GetId(),
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
		Name:      uuid.New().String(),
		DatasetId: datasetID2.GetId(),
	})
	if err != nil {
		log.Fatalln(err.Error())
	}
}
