/*
Copyright © 2024 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/spf13/cobra"
)

// lsCmd represents the ls command
var lsCmd = &cobra.Command{
	Use:   "ls",
	Short: "List S3",

	Run: func(cmd *cobra.Command, args []string) {
		executeLs(args)
	},
}

func init() {
	rootCmd.AddCommand(lsCmd)
}

func executeLs(args []string) {
	client, err := newClient()
	if err != nil {
		if err != nil {
			fmt.Fprintln(os.Stderr, "client error: ", err)
			return
		}
	}

	path := args[0]
	bucket, key, err := extractBucketAndKey(path)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error: ", err)
		return
	}

	if strings.HasSuffix(key, "*") {
		key = key[:len(key)-1]
		err = client.listObject(context.Background(), bucket, key, printObjectDetails)
	} else {
		err = client.listSingleObject(bucket, key)
	}

	if err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
}

func printObjectDetails(object types.Object) {
	fmt.Printf("%s\t%d\t%s\n", object.LastModified, object.Size, aws.ToString(object.Key))
}

func (s *s3client) listSingleObject(bucket, path string) error {
	output, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(path),
	})

	if err != nil {
		return err
	}

	// todo format time and size
	fmt.Printf("%s\t%d\t%s\n", output.LastModified, aws.ToInt64(output.ContentLength), path)
	return nil
}

func (s *s3client) listObject(ctx context.Context, bucket, prefix string, onObject func(types.Object)) error {
	isTruncated := true
	var continuationToken *string

loop:
	for isTruncated {
		select {
		case <-ctx.Done():
			break loop
		default:
			// do nothing
		}

		output, err := s.client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
			Bucket:            aws.String(bucket),
			Prefix:            aws.String(prefix),
			MaxKeys:           aws.Int32(5),
			ContinuationToken: continuationToken,
		})

		if err != nil {
			return err
		}

		if *output.IsTruncated {
			slog.Debug("s3 list pagination", "token", aws.ToString(output.NextContinuationToken), "isTruncated", *output.IsTruncated)
		}

		for _, object := range output.Contents {
			// todo format time and size
			onObject(object)
		}

		isTruncated = *output.IsTruncated
		continuationToken = output.NextContinuationToken
	}

	return nil
}
