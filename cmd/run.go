package cmd

import (
	"github.com/ScienceObjectsDB/CORE-Server/server"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Starts the server for the scienceobjectsdb based on the provided config",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		err := server.Run()
		if err != nil {
			log.Fatalln(err.Error())
		}

	},
}

func init() {
	rootCmd.AddCommand(runCmd)
}
