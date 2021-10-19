package authz

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/ScienceObjectsDB/CORE-Server/models"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"

	"github.com/spf13/viper"
)

type OAuth2Authz struct {
	UserInfoEndpointURL string
	DB                  *gorm.DB
	JwtHandler          *JWTHandler
}

func NewOAuth2Authz(db *gorm.DB, authz *JWTHandler) (*OAuth2Authz, error) {
	endpointURL := viper.GetString("OAuth2.UserInfoEndpoint")
	if endpointURL == "" {
		err := errors.New("endpoint URL has to be provided in config as 'OAuth2.UserInfoEndpoint'")
		log.Println(err.Error())
		return nil, err
	}

	handler := OAuth2Authz{
		UserInfoEndpointURL: endpointURL,
		DB:                  db,
		JwtHandler:          authz,
	}

	return &handler, nil
}

func (handler *OAuth2Authz) Authorize(token string, projectID uuid.UUID) (bool, error) {
	parsedToken, err := handler.JwtHandler.VerifyAndParseToken(token)
	if err != nil {
		log.Println(err.Error())
		return false, errors.New("could not verify token")
	}

	var ok bool
	var claims *CustomClaim

	if claims, ok = parsedToken.Claims.(*CustomClaim); !ok || !parsedToken.Valid {
		return false, errors.New("could not verify token")
	}

	hasGroup := false
	for _, group := range claims.UserGroups {
		if group == "/sciobjsdb-test" {
			hasGroup = true
			break
		}
	}

	if !hasGroup {
		return false, fmt.Errorf("user not part of group sciobjsdb-test")
	}

	user := &models.User{
		UserOauth2ID: claims.Subject,
		ProjectID:    projectID,
	}

	if err := handler.DB.First(user).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return false, fmt.Errorf("could not authorize request")
		}

		log.Println(err.Error())
		return false, err
	}

	return true, nil
}

func (handler *OAuth2Authz) GetUserID(token string) (uuid.UUID, error) {
	req, err := http.NewRequest(
		"GET",
		handler.UserInfoEndpointURL,
		http.NoBody,
	)

	if err != nil {
		log.Println(err.Error())
		return uuid.UUID{}, err
	}

	req.Header.Add("Authorization", "Bearer "+token)

	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return uuid.UUID{}, fmt.Errorf("failed getting user info: %s", err.Error())
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		err := fmt.Errorf("bad reponse when requesting userinfo: %v", response.Status)
		log.Println(err)
		return uuid.UUID{}, err
	}

	contents, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return uuid.UUID{}, fmt.Errorf("failed reading response body: %s", err.Error())
	}

	parsedContents := make(map[string]interface{})
	err = json.Unmarshal(contents, &parsedContents)
	if err != nil {
		log.Println(err.Error())
		return uuid.UUID{}, err
	}

	var ok bool
	var userID interface{}
	if userID, ok = parsedContents["sub"]; !ok {
		return uuid.UUID{}, fmt.Errorf("could not read sub claim from userinfo response")
	}

	userIDString := uuid.MustParse(userID.(string))
	return userIDString, nil
}
