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

type listParams struct {
	bucket    string
	prefix    *string
	delimiter *string
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

	switch {
	case strings.HasSuffix(key, "*"):
		key = strings.TrimSuffix(key, "*")
		params := listParams{bucket: bucket, prefix: aws.String(key)}
		err = client.listObject(context.Background(), params, printObjectDetails)
	case strings.HasSuffix(key, "/"):
		params := listParams{bucket: bucket, prefix: aws.String(key), delimiter: aws.String("/")}
		err = client.listObject(context.Background(), params, printObjectDetails)
	case len(key) == 0:
		params := listParams{bucket: bucket, delimiter: aws.String("/")}
		err = client.listObject(context.Background(), params, printObjectDetails)
	default:
		err = client.listSingleObject(bucket, key)
	}

	if err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
}

func printObjectDetails(output *s3.ListObjectsV2Output) {
	for _, prefix := range output.CommonPrefixes {
		// 29 width empty string for matching width with UTC time formatted object.LastModified
		// 7 width empty string with "PRE" for matching 10 width object.Size
		fmt.Printf("%29s\tPRE%7s\t%s\n", "", "", aws.ToString(prefix.Prefix))
	}

	for _, object := range output.Contents {
		// todo format time and size
		fmt.Printf("%s\t%-10d\t%s\n", aws.ToTime(object.LastModified), aws.ToInt64(object.Size), aws.ToString(object.Key))
	}
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

func (s *s3client) listObject(ctx context.Context, params listParams, onList func(*s3.ListObjectsV2Output)) error {
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
			Bucket:            aws.String(params.bucket),
			Prefix:            params.prefix,
			Delimiter:         params.delimiter,
			ContinuationToken: continuationToken,
		})

		if err != nil {
			return err
		}

		if *output.IsTruncated {
			slog.Debug("s3 list pagination", "token", aws.ToString(output.NextContinuationToken), "isTruncated", *output.IsTruncated)
		}

		onList(output)

		isTruncated = *output.IsTruncated
		continuationToken = output.NextContinuationToken
	}

	return nil
}
