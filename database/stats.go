package database

import (
	"github.com/ScienceObjectsDB/CORE-Server/models"
	v1storagemodels "github.com/ScienceObjectsDB/go-api/sciobjsdb/api/storage/models/v1"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

type Stats struct {
	*Common
}

func (stats *Stats) GetProjectStats(projectID uuid.UUID) (*v1storagemodels.ProjectStats, error) {
	var object_groups_count int64
	var objects_count int64
	var acc_object_size int64
	var avg_objects_size float64
	var user_count int64

	wg := errgroup.Group{}

	wg.Go(func() error {
		err := stats.DB.Model(&models.Object{}).Where("project_id = ?", projectID.String()).Select("count(*) as objects_count").Find(&objects_count).Error
		if err != nil {
			log.Errorln(err.Error())
			return err
		}

		return nil
	})

	wg.Go(func() error {
		err := stats.DB.Model(&models.Object{}).Where("project_id = ?", projectID.String()).Select("coalesce(avg(content_len), -1) as avg_objects_size").Find(&avg_objects_size).Error
		if err != nil {
			log.Errorln(err.Error())
			return err
		}

		return nil
	})

	wg.Go(func() error {
		err := stats.DB.Model(&models.ObjectGroup{}).Where("project_id = ?", projectID.String()).Select("count(*) as object_groups_count").Find(&object_groups_count).Error
		if err != nil {
			log.Errorln(err.Error())
			return err
		}

		return nil
	})

	wg.Go(func() error {
		err := stats.DB.Model(&models.Object{}).Where("project_id = ? and content_len is not null", projectID.String()).Select("coalesce(sum(content_len), -1) as acc_object_size").Find(&acc_object_size).Error
		if err != nil {
			log.Errorln(err.Error())
			return err
		}

		return nil
	})

	wg.Go(func() error {
		err := stats.DB.Model(&models.User{}).Where("project_id = ?", projectID.String()).Select("count(*) as user_count").Find(&user_count).Error
		if err != nil {
			log.Errorln(err.Error())
			return err
		}

		return nil
	})

	err := wg.Wait()
	if err != nil {
		log.Errorln(err.Error())
		return nil, err
	}

	projectStats := &v1storagemodels.ProjectStats{
		ObjectCount:      objects_count,
		ObjectGroupCount: object_groups_count,
		AccSize:          acc_object_size,
		AvgObjectSize:    avg_objects_size,
		UserCount:        user_count,
	}

	return projectStats, nil
}

func (stats *Stats) GetDatasetStats(datasetID uuid.UUID) (*v1storagemodels.DatasetStats, error) {
	var object_groups_count int64
	var objects_count int64
	var acc_object_size int64
	var avg_objects_size float64

	wg := errgroup.Group{}

	wg.Go(func() error {
		err := stats.DB.Model(&models.Object{}).Where("dataset_id = ?", datasetID.String()).Select("count(*) as objects_count").Find(&objects_count).Error
		if err != nil {
			log.Errorln(err.Error())
			return err
		}

		return nil
	})

	wg.Go(func() error {
		err := stats.DB.Model(&models.Object{}).Where("dataset_id = ? and content_len is not null", datasetID.String()).Select("coalesce(avg(content_len), -1) as avg_objects_size").Find(&avg_objects_size).Error
		if err != nil {
			log.Errorln(err.Error())
			return err
		}

		return nil
	})

	wg.Go(func() error {
		err := stats.DB.Model(&models.ObjectGroup{}).Where("dataset_id = ?", datasetID.String()).Select("count(*) as object_groups_count").Find(&object_groups_count).Error
		if err != nil {
			log.Errorln(err.Error())
			return err
		}

		return nil
	})

	wg.Go(func() error {
		err := stats.DB.Model(&models.Object{}).Where("dataset_id = ? and content_len is not null", datasetID.String()).Select("coalesce(sum(content_len), -1) as acc_object_size").Find(&acc_object_size).Error
		if err != nil {
			log.Errorln(err.Error())
			return err
		}

		return nil
	})

	err := wg.Wait()
	if err != nil {
		log.Errorln(err.Error())
		return nil, err
	}

	datasetStats := &v1storagemodels.DatasetStats{
		ObjectCount:      objects_count,
		ObjectGroupCount: object_groups_count,
		AccSize:          acc_object_size,
		AvgObjectSize:    avg_objects_size,
	}

	return datasetStats, nil
}

func (stats *Stats) GetObjectGroupStats(objectgroup *models.ObjectGroup) (*v1storagemodels.ObjectGroupStats, []*v1storagemodels.ObjectStats, error) {
	var objects_count int64
	var acc_object_size int64
	var avg_objects_size float64

	wg := errgroup.Group{}

	wg.Go(func() error {
		err := stats.DB.Model(&models.Object{}).Where("object_group_id = ?", objectgroup.ID.String()).Select("count(*) as objects_count").Find(&objects_count).Error
		if err != nil {
			log.Errorln(err.Error())
			return err
		}

		return nil
	})

	wg.Go(func() error {
		err := stats.DB.Model(&models.Object{}).Where("object_group_id = ? and content_len is not null", objectgroup.ID.String()).Select("coalesce(avg(content_len), -1) as avg_objects_size").Find(&avg_objects_size).Error
		if err != nil {
			log.Errorln(err.Error())
			return err
		}

		return nil
	})

	wg.Go(func() error {
		err := stats.DB.Model(&models.Object{}).Where("object_group_id = ? and content_len is not null", objectgroup.ID.String()).Select("coalesce(sum(content_len), -1) as acc_object_size").Find(&acc_object_size).Error
		if err != nil {
			log.Errorln(err.Error())
			return err
		}

		return nil
	})

	objectStatsList := make([]*v1storagemodels.ObjectStats, len(objectgroup.Objects))

	for i, object := range objectgroup.Objects {
		wg.Go(func() error {
			objectStats, err := stats.GetObjectStats(object.ID)
			if err != nil {
				log.Errorln(err.Error())
				return err
			}

			objectStatsList[i] = objectStats

			return nil
		})
	}

	err := wg.Wait()
	if err != nil {
		log.Errorln(err.Error())
		return nil, nil, err
	}

	objectgroupStats := &v1storagemodels.ObjectGroupStats{
		ObjectCount:   objects_count,
		AccSize:       acc_object_size,
		AvgObjectSize: avg_objects_size,
	}

	return objectgroupStats, objectStatsList, nil
}

func (stats *Stats) GetObjectStats(objectID uuid.UUID) (*v1storagemodels.ObjectStats, error) {

	objectStats := &v1storagemodels.ObjectStats{}
	return objectStats, nil
}

func (stats *Stats) GetDatasetVersionStats(datasetVersion *models.DatasetVersion) (*v1storagemodels.DatasetVersionStats, error) {
	var objects_count int64
	var acc_object_size int64
	var avg_objects_size float64

	wg := errgroup.Group{}

	wg.Go(func() error {
		err := stats.DB.Model(&models.Object{}).Select("count(*) as objects_count").Joins("inner join dataset_version_object_groups on objects.object_group_id = dataset_version_object_groups.object_group_id").Where("dataset_version_id=?", datasetVersion.ID).Find(&objects_count).Error
		if err != nil {
			log.Errorln(err.Error())
			return err
		}

		return nil
	})

	wg.Go(func() error {
		err := stats.DB.Model(&models.Object{}).Select("sum(content_len) as acc_object_size").Joins("inner join dataset_version_object_groups on objects.object_group_id = dataset_version_object_groups.object_group_id").Where("dataset_version_id=?", datasetVersion.ID).Find(&acc_object_size).Error
		if err != nil {
			log.Errorln(err.Error())
			return err
		}

		return nil
	})

	wg.Go(func() error {
		err := stats.DB.Model(&models.Object{}).Select("avg(content_len) as acc_object_size").Joins("inner join dataset_version_object_groups on objects.object_group_id = dataset_version_object_groups.object_group_id").Where("dataset_version_id=?", datasetVersion.ID).Find(&avg_objects_size).Error
		if err != nil {
			log.Errorln(err.Error())
			return err
		}

		return nil
	})

	wg.Wait()

	versionStats := &v1storagemodels.DatasetVersionStats{
		ObjectCount:   objects_count,
		AccSize:       acc_object_size,
		AvgObjectSize: avg_objects_size,
	}

	return versionStats, nil
}
