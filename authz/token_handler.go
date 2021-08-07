package authz

import (
	"github.com/ScienceObjectsDB/CORE-Server/models"
	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

type APITokenHandler struct {
	DB *gorm.DB
}

func (handler *APITokenHandler) GetUserID(token string) (string, error) {
	tokenModel := &models.APIToken{}

	if err := handler.DB.Where("token = ?", token).First(tokenModel).Error; err != nil {
		log.Println(err.Error())
		return "", err
	}

	return tokenModel.UserUUID, nil
}
