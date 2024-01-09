package cmd

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExtractKeyAndBucket(t *testing.T) {
	cases := []struct {
		name       string
		input      string
		wantBucket string
		wantKey    string
	}{
		{"bucket and key", "s3://mybucket/mykey", "mybucket", "mykey"},
		{"bucket and no key", "s3://mybucket/", "mybucket", ""},
		{"bucket and no key and no slash at the end", "s3://mybucket", "mybucket", ""},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			gotBucket, gotKey, err := extractBucketAndKey(c.input)
			require.NoError(t, err)

			if gotBucket != c.wantBucket {
				t.Errorf("got %v want %v", gotBucket, c.wantBucket)
			}

			if gotKey != c.wantKey {
				t.Errorf("got %v want %v", gotKey, c.wantKey)
			}
		})
	}
}

func TestExtractKeyAndBucketForError(t *testing.T) {
	cases := []struct {
		name    string
		input   string
		wantErr error
	}{
		{"not s3 path", "/mybucket/mykey", errNotS3path},
		{"no bucket", "s3://", errNoBucketFound},
		{"empty bucket", "s3:///", errNoBucketFound},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, _, err := extractBucketAndKey(c.input)

			if !errors.Is(err, c.wantErr) {
				t.Errorf("got %v want %v", err, c.wantErr)
			}
		})
	}
}
