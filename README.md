# s3cli

Some S3 utilities

```
Usage:
  s3cli [command]

Available Commands:
  completion  Generate the autocompletion script for the specified shell
  cp          Copy from/to S3
  help        Help about any command
  ls          List S3

Flags:
  -e, --endpoint string             Use alternative endpoint
  -h, --help                        help for s3cli
  -m, --max-parallel-requests int   Number of maximum requests to run in parallel (default 10)
```

### Listing
```$ s3cli ls s3://my-bucket/
PRE  date=2024/

# list all objects under my-bucket
$ s3cli ls s3://my-bucket/*
2024-01-04 12:35:55 +0000 UTC   10533360        date=2024/foo.txt
2024-01-05 08:44:31 +0000 UTC   0               date=2024/deneme/1.txt
2024-01-05 08:44:31 +0000 UTC   0               date=2024/deneme/2.txt
2024-01-05 08:44:31 +0000 UTC   482             date=2024/deneme/part-00000.parquet
2024-01-05 08:44:31 +0000 UTC   2259            date=2024/deneme/part-00046.parquet
2024-01-05 08:44:31 +0000 UTC   8741            date=2024/deneme/part-00053.parquet
```

### Copying
```
# download everytinh under my-bucket to directory temp
$ s3cli cp s3://my-bucket/* temp/
```