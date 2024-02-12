# S3 Simple Benchmarker (for performance measurement purposes)

This tool is designed to measure the performance of upload and download operations with an S3-compatible object storage service.
It calculates metrics such as average time, P90 (90th percentile) time, and P90 speed for both upload and download operations.

## Features

- Measures average upload and download time.
- Calculates P90 upload and download time.
- Calculates P90 upload and download speed.

## Usage

``` sh
$ go get -v ./... && go build && ./s3-simple-benchmarker -h
```

## Configuration

- The application requires specifying the following parameters:
  - S3 endpoint: The endpoint URL of the S3-compatible service.
  - Access key: The access key for accessing the S3-compatible service.
  - Secret key: The secret key associated with the access key.
  - Bucket name: The name of the bucket in the S3-compatible service.

## License

This project is licensed under the MIT License.
