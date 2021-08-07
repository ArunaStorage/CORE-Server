package util

import (
	"fmt"
	"math/rand"

	log "github.com/sirupsen/logrus"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type TestDatabase struct {
	DB *gorm.DB
}

func (testdatabase *TestDatabase) New() (*TestDatabase, error) {
	value := rand.Int63()
	rndDatabaseString := fmt.Sprintf("testdatabase%v", value)

	dsnTmp := "host=localhost user=postgres password=test123 dbname=postgres port=5432 sslmode=disable TimeZone=Europe/Berlin"
	dbTmp, err := gorm.Open(postgres.Open(dsnTmp), &gorm.Config{})
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	resultDrop := dbTmp.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %v", rndDatabaseString))
	if resultDrop.Error != nil {
		log.Println(resultDrop.Error.Error())
		return nil, resultDrop.Error
	}

	result := dbTmp.Exec(fmt.Sprintf("CREATE DATABASE %v", rndDatabaseString))
	if result.Error != nil {
		log.Println(result.Error.Error())
		return nil, result.Error
	}

	tmpDB, err := dbTmp.DB()
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	err = tmpDB.Close()
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	dsn := fmt.Sprintf("host=localhost user=postgres password=test123 dbname=%v port=5432 sslmode=disable TimeZone=Europe/Berlin", rndDatabaseString)
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	err = MakeMigrations(db)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	testdatabase.DB = db

	return testdatabase, nil
}
