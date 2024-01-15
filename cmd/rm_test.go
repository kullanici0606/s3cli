package cmd

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSplitGlobsAndRegulars(t *testing.T) {
	cases := []struct {
		name         string
		input        []string
		wantGlobs    []string
		wantRegulars []string
	}{
		{"all globs", []string{"s3://a/foo/*", "s3://a/bar/*"}, []string{"s3://a/foo/*", "s3://a/bar/*"}, nil},
		{"all regulars", []string{"s3://a/foo/", "s3://a/bar/"}, nil, []string{"s3://a/foo/", "s3://a/bar/"}},
		{"mixed", []string{"s3://a/foo/*", "s3://a/bar/"}, []string{"s3://a/foo/*"}, []string{"s3://a/bar/"}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			gotGlobs, gotRegulars := splitGlobsAndRegulars(c.input)

			if !reflect.DeepEqual(gotGlobs, c.wantGlobs) {
				t.Errorf("got %v want %v", gotGlobs, c.wantGlobs)
			}

			if !reflect.DeepEqual(gotRegulars, c.wantRegulars) {
				t.Errorf("got %v want %v", gotRegulars, c.wantRegulars)
			}
		})
	}
}

func TestGroupByBuckets(t *testing.T) {
	paths := []string{
		"s3://b1/k1",
		"s3://b1/k2",
		"s3://b1/k3",
		"s3://b2/k4",
		"s3://b2/k5",
	}

	got, err := groupByBucket(paths)
	require.NoError(t, err)

	want := map[string][]string{
		"b1": {"k1", "k2", "k3"},
		"b2": {"k4", "k5"},
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v want %v", got, want)
	}

}
