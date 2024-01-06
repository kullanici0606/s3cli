/*
Copyright © 2024 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
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
	s3Cmd.AddCommand(cpCmd)
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

	fnch := make(chan func() error, 10)
	outch := make(chan string, 10)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		err = s.runPooled(fnch)
		if err != nil {
			cancel()
			fmt.Fprintln(os.Stderr, err)
		}
		close(outch)
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		for out := range outch {
			fmt.Println(out)
		}
		wg.Done()
	}()

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
	select {
	case <-dcp.ctx.Done():
		return filepath.SkipAll
	default:
		// do nothing
	}

	if err != nil {
		return err
	}

	if d.IsDir() {
		return nil
	}

	remotepath := dcp.generateRemotePath(path)
	dcp.fnch <- func() error {
		s3path, err := dcp.copyFunc(path, remotepath)
		if err != nil {
			return err
		}

		dcp.outch <- fmt.Sprintf("upload %s %s", path, s3path)
		return nil
	}
	return nil
}

// generate remote path for upload
// dest remote path prefix
// path local path
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
	// TODO implement
	fmt.Fprintln(os.Stderr, "not implemented yet")
	return nil
}

func (s *s3client) copyFromS3ToS3(src, dest string) error {
	// TODO implement
	fmt.Fprintln(os.Stderr, "not implemented yet")
	return nil
}
