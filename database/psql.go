package database

import (
	"fmt"
	"os"

	log "github.com/sirupsen/logrus"

	"github.com/ScienceObjectsDB/CORE-Server/models"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// NewPsqlDB Regular Postgres database init.
// Will check if database migrations are required.
// Also works for cockroachDB
func NewPsqlDB(host string, port uint64, username string, dbName string) (*gorm.DB, error) {
	psqlPW := os.Getenv("PSQL_PASSWORD")
	dsn := fmt.Sprintf("postgres://%v:%v@%v:%v/%v?sslmode=prefer&TimeZone=Europe/Berlin", username, psqlPW, host, port, dbName)

	if psqlPW == "" {
		dsn = "postgres://root@localhost:26257/test?sslmode=disable"
	}

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: NewGormLogger(),
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	db = makeMigrations(db)

	return db, nil
}

// NewPsqlDBCITest SQL connection for CI testing with a local database
func NewPsqlDBCITest() (*gorm.DB, error) {
	dsn := "postgres://root@cockroach:26257/defaultdb?sslmode=disable"

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	db = makeMigrations(db)

	return db, nil
}

func makeMigrations(db *gorm.DB) *gorm.DB {
	log.Println(db.DB())

	err := db.AutoMigrate(
		&models.Project{},
		&models.Dataset{},
		&models.DatasetVersion{},
		&models.ObjectGroup{},
		&models.Object{},
		&models.Location{},
		&models.Metadata{},
		&models.Label{},
		&models.APIToken{},
		&models.User{},
		&models.StreamingEntry{},
	)

	if err != nil {
		log.Fatalln(err.Error())
	}

	return db
}
