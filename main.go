package main

import (
	"fmt"

	log "github.com/sirupsen/logrus"

	"github.com/ScienceObjectsDB/CORE-Server/server"
	"github.com/spf13/viper"
)

func main() {
	log.SetFormatter(&log.JSONFormatter{})
	log.SetReportCaller(true)

	viper.SetConfigName("config")        // name of config file (without extension)
	viper.SetConfigType("yaml")          // REQUIRED if the config file does not have the extension in the name
	viper.AddConfigPath("/etc/appname/") // path to look for the config file in
	viper.AddConfigPath("./config/local")
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
