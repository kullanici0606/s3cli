/*
Copyright © 2024 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
)

// s3Cmd represents the s3 command
var s3Cmd = &cobra.Command{
	Use:   "s3",
	Short: "S3 Utilities",
}

const (
	s3prefix    = "s3://"
	s3prefixLen = len(s3prefix)
)

var errNotS3path = errors.New("not a s3 path")
var errNoBucketFound = errors.New("no bucket found")

var globalS3Endpoint string
var globalMaxParallelRequests int

func init() {
	rootCmd.AddCommand(s3Cmd)
	s3Cmd.PersistentFlags().StringVarP(&globalS3Endpoint, "endpoint", "e", "", "Use alternative endpoint")
	s3Cmd.PersistentFlags().IntVarP(&globalMaxParallelRequests, "max-parallel-requests", "m", 10, "Number of maximum requests to run in parallel")
}

type s3client struct {
	client *s3.Client
}

func newClient() (*s3client, error) {
	optionFuncs := make([]func(*config.LoadOptions) error, 0)
	if len(globalS3Endpoint) != 0 {
		optionFuncs = append(optionFuncs, func(options *config.LoadOptions) error {
			options.EndpointResolverWithOptions = aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{
					URL:               globalS3Endpoint,
					HostnameImmutable: true,
				}, nil
			})
			return nil
		})
	}

	cfg, err := config.LoadDefaultConfig(context.Background(), optionFuncs...)
	if err != nil {
		return nil, fmt.Errorf("cannot read config %w", err)
	}

	client := s3.NewFromConfig(cfg)
	return &s3client{client: client}, nil
}

func (s *s3client) runPooled(fnch <-chan func() error) error {
	errg := new(errgroup.Group)

	for i := 0; i < globalMaxParallelRequests; i++ {
		errg.Go(func() error {
			for {
				fn, open := <-fnch
				if !open {
					break
				}

				err := fn()
				if err != nil {
					return err
				}
			}
			return nil
		})
	}

	if err := errg.Wait(); err != nil {
		return err
	}

	return nil
}

func extractBucketAndKey(path string) (bucket, key string, err error) {
	if !strings.HasPrefix(path, s3prefix) {
		return "", "", errNotS3path
	}

	path = path[s3prefixLen:]
	idx := strings.IndexRune(path, '/')
	if idx == -1 {
		return "", "", errNoBucketFound
	}

	return path[:idx], path[idx+1:], nil
}

func generateS3Path(bucket, key string) string {
	return fmt.Sprintf("%s%s/%s", s3prefix, bucket, key)
}
