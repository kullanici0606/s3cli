/*
Copyright © 2024 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/spf13/cobra"
)

// rmCmd represents the rm command
var rmCmd = &cobra.Command{
	Use:   "rm",
	Short: "Remove S3 files",
	Run: func(cmd *cobra.Command, args []string) {
		removeS3(args)
	},
}

func init() {
	rootCmd.AddCommand(rmCmd)
}

func removeS3(paths []string) {
	client, err := newClient()
	if err != nil {
		fmt.Fprintln(os.Stderr, "Cannot create s3 client", err)
		return
	}

	globs, regulars := splitGlobsAndRegulars(paths)
	err = client.removePaths(regulars)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error while removing keys", err)
		return
	}

	err = client.removeGlobs(globs)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error while removing keys", err)
		return
	}
}

func splitGlobsAndRegulars(paths []string) (globs, regulars []string) {
	for _, p := range paths {
		if strings.HasSuffix(p, "*") {
			globs = append(globs, p)
			continue
		}
		regulars = append(regulars, p)
	}
	return
}

// group paths by bucket name i.e b1: [k1,k2,k3], b2: [k4, k5]
func groupByBucket(paths []string) (map[string][]string, error) {
	groupByBucket := make(map[string][]string)
	for _, p := range paths {
		bucket, key, err := extractBucketAndKey(p)
		if err != nil {
			return nil, err
		}
		groupByBucket[bucket] = append(groupByBucket[bucket], key)
	}
	return groupByBucket, nil
}

func (s *s3client) removeGlobs(paths []string) error {
	bucketGroups, err := groupByBucket(paths)
	if err != nil {
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fnch := make(chan func() error, globalMaxParallelRequests)
	outch := make(chan string, globalMaxParallelRequests)
	wg := s.runPooled(cancel, fnch, outch)

	for b, keys := range bucketGroups {
		for _, k := range keys {
			fnch <- func() error {
				err := s.removeGlob(ctx, b, k, outch)
				if err != nil {
					return err
				}
				outch <- fmt.Sprintf("%s deleted", generateS3Path(b, k))
				return err
			}
		}
	}

	close(fnch)
	wg.Wait()
	return nil
}

func (s *s3client) removeGlob(ctx context.Context, bucket, prefix string, outch chan<- string) error {
	prefix = strings.TrimSuffix(prefix, "*")
	return s.listObject(ctx, listParams{bucket: bucket, prefix: aws.String(prefix)}, func(output *s3.ListObjectsV2Output) {
		keys := make([]string, 0, len(output.Contents))
		for _, o := range output.Contents {
			keys = append(keys, aws.ToString(o.Key))
		}
		deleteOutput, err := s.removeObjects(bucket, keys)
		if err != nil {
			return
		}

		for _, d := range deleteOutput.Deleted {
			outch <- fmt.Sprintf("%s deleted", aws.ToString(d.Key))
		}

		for _, err := range deleteOutput.Errors {
			outch <- fmt.Sprintf("Error while deleting %s: %s", aws.ToString(err.Key), aws.ToString(err.Message))
		}

	})
}

func (s *s3client) removePaths(paths []string) error {
	bucketGroups, err := groupByBucket(paths)
	if err != nil {
		return nil
	}
	for b, keys := range bucketGroups {
		output, err := s.removeObjects(b, keys)
		if err != nil {
			return err
		}

		for _, d := range output.Deleted {
			fmt.Printf("%s deleted\n", aws.ToString(d.Key))
		}

		for _, err := range output.Errors {
			fmt.Fprintf(os.Stderr, "Error while deleting %s: %s\n", aws.ToString(err.Key), aws.ToString(err.Message))
		}
	}

	return nil
}

func (s *s3client) removeObjects(bucket string, keys []string) (*s3.DeleteObjectsOutput, error) {
	delete := types.Delete{}
	for _, k := range keys {
		delete.Objects = append(delete.Objects, types.ObjectIdentifier{Key: aws.String(k)})
	}

	output, err := s.client.DeleteObjects(context.Background(), &s3.DeleteObjectsInput{
		Bucket: aws.String(bucket),
		Delete: &delete,
	})
	if err != nil {
		return output, err
	}
	return output, nil
}
