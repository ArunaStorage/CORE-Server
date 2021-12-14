package main

import (
	"fmt"
	"os"

	log "github.com/sirupsen/logrus"

	"github.com/ScienceObjectsDB/CORE-Server/server"
	"github.com/spf13/viper"
)

const envLogLevel = "LOG_LEVEL"
const defaultLogLevel = log.WarnLevel

func main() {
	logLevel := getLogLevel()

	log.SetLevel(logLevel)
	log.SetReportCaller(true)

	viper.SetConfigName("config") // name of config file (without extension)
	viper.SetConfigType("yaml")   // REQUIRED if the config file does not have the extension in the name
	viper.AddConfigPath("./config")
	err := viper.ReadInConfig() // Find and read the config file
	if err != nil {             // Handle errors reading the config file
		panic(fmt.Errorf("fatal error config file: %w", err))
	}

	host := viper.GetString("Server.Host")
	port := viper.GetUint("Server.Port")

	err = server.Run(host, uint16(port))
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
