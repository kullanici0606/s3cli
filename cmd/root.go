/*
Copyright © 2024 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"os"

	"github.com/spf13/cobra"
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "v2",
	Short: "Some S3 utilities",
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

var globalS3Endpoint string
var globalMaxParallelRequests int

func init() {
	rootCmd.PersistentFlags().StringVarP(&globalS3Endpoint, "endpoint", "e", "", "Use alternative endpoint")
	rootCmd.PersistentFlags().IntVarP(&globalMaxParallelRequests, "max-parallel-requests", "m", 10, "Number of maximum requests to run in parallel")
}
