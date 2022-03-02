package cmd

import (
	"github.com/ScienceObjectsDB/CORE-Server/database"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var migrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: "Performs migrations for the database",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		err := database.MakeMigrationsStandalone()
		if err != nil {
			log.Fatalln(err.Error())
		}

	},
}

func init() {
	rootCmd.AddCommand(migrateCmd)
}
