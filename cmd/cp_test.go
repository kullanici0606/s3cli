package cmd

import (
	"context"
	"errors"
	"io/fs"
	"path/filepath"
	"testing"
)

type copyOperation int

const (
	localToS3 = iota
	s3ToLocal
	s3toS3
)

func (c copyOperation) String() string {
	switch c {
	case localToS3:
		return "localToS3"
	case s3ToLocal:
		return "s3ToLocal"
	case s3toS3:
		return "s3toS3"
	default:
		return "unknown"
	}

}

type recordingClient struct {
	op copyOperation
}

func (r *recordingClient) copyFromS3ToS3(src, dest string) error {
	r.op = s3toS3
	return nil
}

func (r *recordingClient) copyFromS3ToLocal(src, dest string) error {
	r.op = s3ToLocal
	return nil
}

func (r *recordingClient) copyFromLocalToS3(src, dest string) error {
	r.op = localToS3
	return nil
}

func TestExecuteCopy(t *testing.T) {
	cases := []struct {
		name      string
		inputSrc  string
		inputDest string
		want      copyOperation
	}{
		{"local to s3", "/tmp/test.txt", "s3://bucket/", localToS3},
		{"s3 to local", "s3://bucket/", "/tmp/test.txt", s3ToLocal},
		{"s3 to s3", "s3:///tmp/test.txt", "s3://bucket/", s3toS3},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			client := recordingClient{}
			err := executeCopy(&client, c.inputSrc, c.inputDest)
			if err != nil {
				t.Errorf("Error unwanted here %s", err)
			}

			if client.op != c.want {
				t.Errorf("got %v want %v", client.op, c.want)
			}
		})
	}
}

func TestExecuteCopyLocalToLocal(t *testing.T) {
	var client *recordingClient
	err := executeCopy(client, "/tmp/foo.txt", "/tmp/bar.txt")
	if err == nil {
		t.Errorf("Error expected here %s", err)
	}
}

type dirEntry struct {
	name string
	dir  bool
}

func (d dirEntry) Name() string               { return d.name }
func (d dirEntry) IsDir() bool                { return d.dir }
func (d dirEntry) Info() (fs.FileInfo, error) { return nil, nil }
func (d dirEntry) Type() fs.FileMode          { return 0755 }

func TestWalkDirFuncCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	dcp := directoryCopier{
		ctx: ctx,
	}

	err := dcp.walkDirFunc("/tmp", dirEntry{dir: false}, nil)
	if !errors.Is(err, filepath.SkipAll) {
		t.Errorf("SkipAll error expected here %s", err)
	}
}

func TestGenerateRemotePath(t *testing.T) {
	cases := []struct {
		name               string
		inputDest          string
		inputSrcRoot       string
		inputPath          string
		inputPathSeparator rune
		want               string
	}{
		{"unix style", "s3://bucket/outputs", "outputs", "outputs/1.txt", '/', "s3://bucket/outputs/1.txt"},
		{"unix style - trailing slash", "s3://bucket/outputs", "outputs/", "outputs/1.txt", '/', "s3://bucket/outputs/1.txt"},
		{"windows style", "s3://bucket/outputs", "outputs", "outputs\\1.txt", '\\', "s3://bucket/outputs/1.txt"},
		{"windows style - trailing slash", "s3://bucket/outputs", "outputs\\", "outputs\\1.txt", '\\', "s3://bucket/outputs/1.txt"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dcp := directoryCopier{
				dest:          c.inputDest,
				srcRoot:       c.inputSrcRoot,
				pathSeparator: c.inputPathSeparator,
			}

			got := dcp.generateRemotePath(c.inputPath)

			if got != c.want {
				t.Errorf("got %v want %v", got, c.want)
			}
		})
	}
}

func TestExtractS3FileName(t *testing.T) {
	cases := []struct {
		name  string
		input string
		want  string
	}{
		{"no delimiter", "test.txt", "test.txt"},
		{"with delimiter", "foo/bar/test.txt", "test.txt"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := extractS3FileName(c.input)
			if got != c.want {
				t.Errorf("got %v want %v", got, c.want)
			}
		})
	}
}

func TestConvertToLocalPath(t *testing.T) {
	cases := []struct {
		name        string
		inputPrefix string
		inputKey    string
		inputDest   string
		want        string
	}{
		{"key with multiple delimiters", "prefix", "prefix/foo/bar/test.txt", "/tmp", "/tmp/foo/bar/test.txt"},
		{"key with multiple delimiters with trailing slash", "prefix/", "prefix/foo/bar/test.txt", "/tmp", "/tmp/foo/bar/test.txt"},
		{"key directly under prefix", "prefix", "prefix/test.txt", "/tmp", "/tmp/test.txt"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := convertToLocalPath(c.inputPrefix, c.inputKey, c.inputDest)
			if got != c.want {
				t.Errorf("got %v want %v", got, c.want)
			}
		})
	}
}
