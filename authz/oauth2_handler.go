package authz

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/ScienceObjectsDB/CORE-Server/models"
	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"

	"github.com/spf13/viper"
)

type OAuth2Authz struct {
	UserInfoEndpointURL string
	DB                  *gorm.DB
}

type Oauth2User struct {
	sub    string
	groups []string
}

func NewOAuth2Authz(db *gorm.DB) (*OAuth2Authz, error) {
	endpointURL := viper.GetString("OAuth2.UserInfoEndpoint")
	if endpointURL == "" {
		err := errors.New("endpoint URL has to be provided in config as 'OAuth2.UserInfoEndpoint'")
		log.Println(err.Error())
		return nil, err
	}

	handler := OAuth2Authz{
		UserInfoEndpointURL: endpointURL,
		DB:                  db,
	}

	return &handler, nil
}

func (handler *OAuth2Authz) Authorize(token string, projectID uint) (bool, error) {
	oauth2User, err := handler.ParseUser(token)
	if err != nil {
		log.Println(err.Error())
		return false, err
	}

	hasGroup := false
	for _, group := range oauth2User.groups {
		log.Println(group)
		if group == "/sciobjsdb-test" {
			hasGroup = true
			break
		}
	}

	if !hasGroup {
		return false, fmt.Errorf("user not part of group sciobjsdb-test")
	}

	user := &models.User{
		UserOauth2ID: oauth2User.sub,
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

func (handler *OAuth2Authz) ParseUser(token string) (*Oauth2User, error) {
	req, err := http.NewRequest(
		"GET",
		handler.UserInfoEndpointURL,
		http.NoBody,
	)

	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	req.Header.Add("Authorization", "Bearer "+token)

	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed getting user info: %s", err.Error())
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		err := fmt.Errorf("bad reponse when requesting userinfo: %v", response.Status)
		log.Println(err)
		return nil, err
	}

	contents, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, fmt.Errorf("failed reading response body: %s", err.Error())
	}

	parsedContents := &Oauth2User{}
	err = json.Unmarshal(contents, &parsedContents)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	return parsedContents, nil
}

func (handler *OAuth2Authz) GetUserID(token string) (string, error) {
	req, err := http.NewRequest(
		"GET",
		handler.UserInfoEndpointURL,
		http.NoBody,
	)

	if err != nil {
		log.Println(err.Error())
		return "", err
	}

	req.Header.Add("Authorization", "Bearer "+token)

	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed getting user info: %s", err.Error())
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		err := fmt.Errorf("bad reponse when requesting userinfo: %v", response.Status)
		log.Println(err)
		return "", err
	}

	contents, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return "", fmt.Errorf("failed reading response body: %s", err.Error())
	}

	parsedContents := make(map[string]interface{})
	err = json.Unmarshal(contents, &parsedContents)
	if err != nil {
		log.Println(err.Error())
		return "", err
	}

	var ok bool
	var userID interface{}
	if userID, ok = parsedContents["sub"]; !ok {
		return "", fmt.Errorf("could not read sub claim from userinfo response")
	}

	userIDString := userID.(string)
	return userIDString, nil
}
