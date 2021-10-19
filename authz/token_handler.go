package authz

import (
	"fmt"

	"github.com/ScienceObjectsDB/CORE-Server/models"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

type APITokenHandler struct {
	DB *gorm.DB
}

func (handler *APITokenHandler) GetUserID(token string) (uuid.UUID, error) {
	tokenModel := &models.APIToken{}

	if err := handler.DB.Where("token = ?", token).First(tokenModel).Error; err != nil {
		log.Println(err.Error())
		return uuid.UUID{}, err
	}

	return tokenModel.UserUUID, nil
}

func (handler *APITokenHandler) Authorize(token string, projectID uuid.UUID) (bool, error) {

	tokenModel := &models.APIToken{}
	if err := handler.DB.Where("token = ? AND project_id = ?", token, projectID).First(tokenModel).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return false, fmt.Errorf("could not authorize request")
		}

		log.Println(err.Error())
		return false, err
	}

	return true, nil
}
