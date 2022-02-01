package main

import (
	"os"

	log "github.com/sirupsen/logrus"

	"github.com/ScienceObjectsDB/CORE-Server/config"
	"github.com/ScienceObjectsDB/CORE-Server/server"
	"github.com/spf13/viper"
)

const envLogLevel = "LOG_LEVEL"
const defaultLogLevel = log.WarnLevel

func main() {
	config.HandleConfigFile()

	logLevel := getLogLevel()

	log.SetLevel(logLevel)
	log.SetReportCaller(true)

	host := viper.GetString(config.SERVER_HOST)
	port := viper.GetUint(config.SERVER_PORT)

	err := server.Run(host, uint16(port))
	if err != nil {
		log.Fatalln(err.Error())
	}

}

func getLogLevel() log.Level {
	levelString, exists := os.LookupEnv(envLogLevel)
	if !exists {
		return defaultLogLevel
	}

	level, err := log.ParseLevel(levelString)
	if err != nil {
		log.Fatalln(err.Error())
	}

	return level
}
