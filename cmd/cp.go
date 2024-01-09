/*
Copyright © 2024 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/spf13/cobra"
)

// cpCmd represents the cp command
var cpCmd = &cobra.Command{
	Use:   "cp",
	Short: "Copy from/to S3",
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		executeCp(args)
	},
}

var globalFlatten bool

func init() {
	rootCmd.AddCommand(cpCmd)
	cpCmd.Flags().BoolVarP(&globalFlatten, "flatten", "f", false, "flatten directory tree")
}

type s3CopyClient interface {
	copyFromS3ToS3(src, dest string) error
	copyFromS3ToLocal(src, dest string) error
	copyFromLocalToS3(src, dest string) error
}

func executeCp(args []string) {
	src, dest := args[0], args[1]

	client, err := newClient()
	if err != nil {
		if err != nil {
			fmt.Fprintln(os.Stderr, "client error: ", err)
			return
		}
	}

	err = executeCopy(client, src, dest)

	if err != nil {
		fmt.Fprintln(os.Stderr, "copy error: ", err)
	}
}

func executeCopy(client s3CopyClient, src, dest string) error {
	switch {
	case strings.HasPrefix(src, s3prefix) && strings.HasPrefix(dest, s3prefix):
		return client.copyFromS3ToS3(src, dest)
	case strings.HasPrefix(src, s3prefix):
		return client.copyFromS3ToLocal(src, dest)
	case strings.HasPrefix(dest, s3prefix):
		return client.copyFromLocalToS3(src, dest)
	default:
		return errors.New("local to local copy is not supported")
	}
}

func (s *s3client) copyFromLocalToS3(src, dest string) error {
	info, err := os.Stat(src)
	if err != nil {
		return err
	}

	if !info.IsDir() {
		path, err := s.copySingleToS3(src, dest)
		if err != nil {
			return err
		}
		fmt.Printf("upload %s %s\n", src, path)
		return nil
	}

	fnch := make(chan func() error, globalMaxParallelRequests)
	outch := make(chan string, globalMaxParallelRequests)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wg := s.runPooled(cancel, fnch, outch)

	if strings.HasSuffix(dest, "/") {
		dest += info.Name()
	} else {
		dest = dest + "/" + info.Name()
	}

	dcp := &directoryCopier{
		dest:          dest,
		srcRoot:       src,
		fnch:          fnch,
		outch:         outch,
		copyFunc:      s.copySingleToS3,
		ctx:           ctx,
		pathSeparator: filepath.Separator,
	}

	err = filepath.WalkDir(src, dcp.walkDirFunc)

	close(fnch)
	wg.Wait()

	if err != nil {
		return err
	}

	return nil
}

type directoryCopier struct {
	dest          string
	srcRoot       string
	fnch          chan<- func() error
	outch         chan<- string
	ctx           context.Context
	copyFunc      func(string, string) (string, error)
	pathSeparator rune
}

func (dcp *directoryCopier) walkDirFunc(path string, d fs.DirEntry, err error) error {
	if err != nil {
		return err
	}

	if d.IsDir() {
		return nil
	}

	remotepath := dcp.generateRemotePath(path)

	select {
	case <-dcp.ctx.Done():
		return filepath.SkipAll
	case dcp.fnch <- func() error {
		s3path, err := dcp.copyFunc(path, remotepath)
		if err != nil {
			return err
		}
		dcp.outch <- fmt.Sprintf("upload %s %s", path, s3path)
		return nil
	}:
		return nil
	}

}

// generate remote path from local path for upload
func (dcp *directoryCopier) generateRemotePath(path string) string {
	subpath := strings.TrimPrefix(path, dcp.srcRoot)
	if dcp.pathSeparator != '/' {
		subpath = strings.ReplaceAll(subpath, string(dcp.pathSeparator), "/")
	}

	if strings.HasPrefix(subpath, "/") {
		return dcp.dest + subpath
	} else {
		return dcp.dest + "/" + subpath
	}
}

func (s *s3client) copySingleToS3(src, dest string) (string, error) {
	bucket, key, err := extractBucketAndKey(dest)
	if err != nil {
		return "", err
	}

	f, err := os.Open(src)
	if err != nil {
		return "", err
	}
	defer f.Close()

	if strings.HasSuffix(key, "/") {
		key += f.Name()
	}

	_, err = s.client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   f,
	})

	if err != nil {
		return "", err
	}

	return generateS3Path(bucket, key), nil
}

func (s *s3client) copyFromS3ToLocal(src, dest string) error {
	if !strings.HasSuffix(src, "/") && !strings.HasSuffix(src, "*") {
		path, err := s.copySingleFromS3ToLocal(src, dest)
		if err != nil {
			return err
		}
		fmt.Printf("Download %s to %s\n", src, path)
		return nil
	}

	src = strings.TrimSuffix(src, "*")

	bucket, prefix, err := extractBucketAndKey(src)
	if err != nil {
		return err
	}

	fnch := make(chan func() error, globalMaxParallelRequests)
	outch := make(chan string, globalMaxParallelRequests)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wg := s.runPooled(cancel, fnch, outch)

	lsParams := listParams{bucket: bucket, prefix: aws.String(prefix)}
	s.listObject(ctx, lsParams, func(output *s3.ListObjectsV2Output) {
	loop:
		for _, o := range output.Contents {
			select {
			case <-ctx.Done():
				break loop
			default:
				s.enqueuForDownload(ctx, bucket, o, src, prefix, dest, fnch, outch)
			}
		}
	})

	close(fnch)
	wg.Wait()

	if err != nil {
		return err
	}

	return nil
}

func (s *s3client) enqueuForDownload(ctx context.Context, bucket string, o types.Object, src, prefix, dest string, fnch chan func() error, outch chan string) {
	select {
	case <-ctx.Done():
		return
	case fnch <- func() error {
		if globalFlatten {
			path, err := s.downloadFile(bucket, aws.ToString(o.Key), dest)
			if err != nil {
				return err
			}
			outch <- fmt.Sprintf("Download %s to %s", src, path)
			return nil
		}

		path := convertToLocalPath(prefix, aws.ToString(o.Key), dest)
		err := os.MkdirAll(filepath.Dir(path), 0755)
		if err != nil {
			return err
		}

		path, err = s.downloadFile(bucket, aws.ToString(o.Key), path)
		if err != nil {
			return err
		}
		srcPath := src + "/" + aws.ToString(o.Key)
		outch <- fmt.Sprintf("Download %s to %s", srcPath, path)
		return nil
	}:
		// noop
	}
}

// convert aws key excluding prefix to local path under dest as file stored subfolders
func convertToLocalPath(prefix, key, dest string) string {
	p := strings.TrimPrefix(key, prefix)
	cols := []string{dest}
	cols = append(cols, strings.Split(p, "/")...)
	return filepath.Join(cols...)
}

func (s *s3client) copySingleFromS3ToLocal(src, dest string) (string, error) {
	bucket, key, err := extractBucketAndKey(src)
	if err != nil {
		return "", err
	}

	return s.downloadFile(bucket, key, dest)
}

func (s *s3client) downloadFile(bucket string, key string, dest string) (string, error) {
	output, err := s.client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})

	if err != nil {
		return "", err
	}

	defer output.Body.Close()

	isdir, err := isDirectory(dest)
	if err != nil {
		return "", err
	}

	if isdir {
		dest = filepath.Join(dest, extractS3FileName(key))
	}

	f, err := os.Create(dest)
	if err != nil {
		return "", err
	}
	defer f.Close()

	io.Copy(f, output.Body)
	return dest, nil
}

func (s *s3client) copyFromS3ToS3(src, dest string) error {
	// TODO implement
	fmt.Fprintln(os.Stderr, "not implemented yet")
	return nil
}

var errDirectoryNotExists = errors.New("directory does not exist")

func isDirectory(name string) (bool, error) {
	info, err := os.Stat(name)
	if err == nil {
		return info.IsDir(), nil
	}

	if os.IsNotExist(err) && strings.HasSuffix(name, string(filepath.Separator)) {
		return false, errDirectoryNotExists
	}

	if os.IsNotExist(err) {
		return false, nil
	}

	return false, err
}

func extractS3FileName(s3key string) string {
	idx := strings.LastIndexByte(s3key, '/')
	if idx == -1 {
		return s3key
	}

	return s3key[idx+1:]
}
