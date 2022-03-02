package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	log "github.com/sirupsen/logrus"
)

const (
	SERVER_HOST = "Server.Host"
	SERVER_PORT = "Server.Port"

	DB_DATABASETYPE = "DB.Databasetype"

	DB_ROACH_HOSTNAME       = "DB.Cockroach.Hostname"
	DB_ROACH_PORT           = "DB.Cockroach.Port"
	DB_ROACH_USER           = "DB.Cockroach.Username"
	DB_ROACH_DATABASENAME   = "DB.Cockroach.Databasename"
	DB_ROACH_PASSWORDENVVAR = "DB.Cockroach.PasswordEnvVar"

	DB_POSTGRES_HOSTNAME       = "DB.Postgres.Hostname"
	DB_POSTGRES_PORT           = "DB.Postgres.Port"
	DB_POSTGRES_USER           = "DB.Postgres.Username"
	DB_POSTGRES_DATABASENAME   = "DB.Postgres.Databasename"
	DB_POSTGRES_PASSWORDENVVAR = "DB.Postgres.PasswordEnvVar"

	S3_BUCKET_PREFIX  = "S3.BucketPrefix"
	S3_ENDPOINT       = "S3.Endpoint"
	S3_IMPLEMENTATION = "S3.Implementation"

	EVENTNOTIFICATION_BACKEND               = "EventNotifications.Backend"
	EVENTNOTIFICATION_NATS_HOST             = "EventNotifications.NATS.HOST"
	EVENTNOTIFICATION_NATS_SUBJECTPREFIX    = "EventNotifications.NATS.SubjectPrefix"
	EVENTNOTIFICATION_NATS_NKeySeedFileName = "EventNotifications.NATS.NKeySeedFileName"
	EVENTNOTIFICATION_NATS_STREAM_NAME      = "EventNotifications.NATS.StreamName"

	AUTHENTICATION_TYPE                     = "Authentication.Type"
	AUTHENTICATION_OAUTH2_USERINFOENDPOINT  = "Authentication.OIDC.UserInfoEndpoint"
	AUTHENTICATION_OAUTH2_REALMINFOENDPOINT = "Authentication.OIDC.RealmInfoEndpoint"

	STREAMING_ENDPOINT       = "Streaming.Endpoint"
	STREAMING_PORT           = "Streaming.Port"
	STREAMING_SECRET_ENV_VAR = "Streaming.SecretEnvVar"
)

const envLogLevel = "LOG_LEVEL"
const defaultLogLevel = log.WarnLevel

var cfgFile string
var rootCmd = &cobra.Command{
	Use:   "scienceobjectsdb",
	Short: "Scienceobjectsdb is a general purpose and scalable data management service.",
	Long:  `Scienceobjectsdb is a general purpose and scalable data management service.`,
	Run: func(cmd *cobra.Command, args []string) {
		// Do Stuff Here
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(HandleConfigFile)
	rootCmd.PersistentFlags().StringVarP(&cfgFile, "config", "c", "./config/config.yaml", "config file (default is ./config/config.yaml)")
}

func HandleConfigFile() {
	SetDefaults()

	logLevel := getLogLevel()

	log.SetLevel(logLevel)
	log.SetReportCaller(true)

	log.Errorln(cfgFile)
	viper.SetConfigFile(cfgFile)

	viper.SetConfigName("config") // name of config file (without extension)
	viper.SetConfigType("yaml")   // REQUIRED if the config file does not have the extension in the name
	viper.AddConfigPath("./config")
	err := viper.ReadInConfig() // Find and read the config file
	if err != nil {             // Handle errors reading the config file
		fmt.Println("Can't read config:", err)
		os.Exit(1)
	}
}

func ConfigEnvVars() {
	viper.SetEnvPrefix("SCIOBJSDB_CONFIG")

	viper.BindEnv("SCIOBJSDB_CONFIG_S3.Implementation")
}

func SetDefaults() {
	viper.SetDefault(SERVER_HOST, "0.0.0.0")
	viper.SetDefault(SERVER_PORT, 50051)

	viper.SetDefault(DB_DATABASETYPE, "Cockroach")

	viper.SetDefault(DB_ROACH_HOSTNAME, "localhost")
	viper.SetDefault(DB_ROACH_PORT, 26257)
	viper.SetDefault(DB_ROACH_USER, "root")
	viper.SetDefault(DB_ROACH_DATABASENAME, "defaultdb")
	viper.SetDefault(DB_ROACH_PASSWORDENVVAR, "PSQL_PASSWORD")

	viper.SetDefault(DB_POSTGRES_HOSTNAME, "localhost")
	viper.SetDefault(DB_POSTGRES_PORT, 26257)
	viper.SetDefault(DB_POSTGRES_USER, "root")
	viper.SetDefault(DB_POSTGRES_DATABASENAME, "defaultdb")
	viper.SetDefault(DB_POSTGRES_PASSWORDENVVAR, "PSQL_PASSWORD")

	viper.SetDefault(S3_BUCKET_PREFIX, "scienceobjectsdb")
	viper.SetDefault(S3_ENDPOINT, "http://localhost:9000")
	viper.SetDefault(S3_IMPLEMENTATION, "generic")

	viper.SetDefault(EVENTNOTIFICATION_BACKEND, "Empty")
	viper.SetDefault(EVENTNOTIFICATION_NATS_HOST, "http://localhost:4222")
	viper.SetDefault(EVENTNOTIFICATION_NATS_SUBJECTPREFIX, "UPDATES")
	viper.SetDefault(EVENTNOTIFICATION_NATS_STREAM_NAME, "UPDATES")

	viper.SetDefault(STREAMING_ENDPOINT, "localhost")
	viper.SetDefault(STREAMING_PORT, 443)
	viper.SetDefault(STREAMING_SECRET_ENV_VAR, "STREAMING_SECRET")

	viper.SetDefault(AUTHENTICATION_TYPE, "INSECURE")
	viper.SetDefault(AUTHENTICATION_OAUTH2_USERINFOENDPOINT, "localhost:9051/auth/realms/DEFAULTREALM/protocol/openid-connect/userinfo")
	viper.SetDefault(AUTHENTICATION_OAUTH2_REALMINFOENDPOINT, "localhost:9051/auth/realms/DEFAULTREALM")

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
