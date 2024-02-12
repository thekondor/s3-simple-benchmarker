// This file is a part of `github.com/thekondor/s3-simple-benchmarker`
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

const (
	accessKeyEnvVarName = `S3_ACCESS_KEY`
	secretKeyEnvVarName = `S3_SECRET_KEY`
)

func main() {
	var (
		endpoint, accessKey, secretKey, bucketName string
		fileSizeMb                                 int
		trials                                     int
	)
	flag.StringVar(&endpoint, "endpoint", "", "S3 endpoint")
	flag.StringVar(&accessKey, "accessKey", "", fmt.Sprintf(`S3 access key (or through $%s)`, accessKeyEnvVarName))
	flag.StringVar(&secretKey, "secretKey", "", fmt.Sprintf(`S3 secret key (or through $%s)`, secretKeyEnvVarName))
	flag.StringVar(&bucketName, "bucketName", "", "S3 bucket name")
	flag.IntVar(&fileSizeMb, "fileSize", 10, "Size of random file to generate and upload (Mb)")
	flag.IntVar(&trials, "trials", 10, "Amount of uploads-downloads")
	flag.Parse()

	if accessKey == "" {
		accessKey = os.Getenv(accessKeyEnvVarName)
	}
	if secretKey == "" {
		secretKey = os.Getenv(secretKeyEnvVarName)
	}

	if endpoint == "" || accessKey == "" || secretKey == "" || bucketName == "" {
		fmt.Printf(`Either endpoint, access key, secret key or bucket name is missing. Run with "-h" to see the usage.`)
		os.Exit(1)
	}

	fileSizeMb *= 1024 * 1024

	minioClient, err := newMinioClient(endpoint, accessKey, secretKey)
	if err != nil {
		log.Fatalf(`Error creating MinIO client: %v`, err)
	}

	fmt.Println(`Upload:`)
	uploadTimes, uploadSpeeds := uploadFiles(minioClient, bucketName, fileSizeMb, trials)
	fmt.Println(`Download:`)
	downloadTimes, downloadSpeeds := downloadFiles(minioClient, bucketName, int64(fileSizeMb), trials)

	var report Report
	report.Avg.UploadTime = calculateAverage(uploadTimes)
	report.Avg.DownloadTime = calculateAverage(downloadTimes)

	report.P90.UploadTime = calculateP90(uploadTimes)
	report.P90.UploadSpeed = calculateP90(uploadSpeeds)

	report.P90.DownloadTime = calculateP90(downloadTimes)
	report.P90.DownloadSpeed = calculateP90(downloadSpeeds)

	fmt.Printf("\nReport:\n%s\n", report)
}

type Report struct {
	Avg struct {
		DownloadTime time.Duration
		UploadTime   time.Duration
	}
	P90 struct {
		UploadTime    time.Duration
		UploadSpeed   float64
		DownloadTime  time.Duration
		DownloadSpeed float64
	}
}

func (r Report) String() string {
	return fmt.Sprintf(` Upload P90  : time=%v speed=%.2f MB/s
 Download P90: time=%v speed=%.2f MB/s
 Average     : upload.time=%v download.time=%v
`,
		r.P90.UploadTime, r.P90.UploadSpeed,
		r.P90.DownloadTime, r.P90.DownloadSpeed,
		r.Avg.UploadTime, r.Avg.DownloadTime)
}

func calculateAverage(times []time.Duration) time.Duration {
	var totalTime time.Duration
	for _, t := range times {
		totalTime += t
	}
	return totalTime / time.Duration(len(times))
}

func calculateP90[T any](values []T) T {
	sort.Slice(values, func(i, j int) bool {
		switch any(values).(type) {
		case []time.Duration:
			return any(values[i]).(time.Duration) < any(values[j]).(time.Duration)
		case []float64:
			return any(values[i]).(float64) < any(values[j]).(float64)
		default:
			panic("Unsupported type")
		}
	})
	p90Index := int(0.9 * float64(len(values)))
	return values[p90Index]
}

func uploadFiles(minioClient *minio.Client, bucketName string, fileSize, numFiles int) ([]time.Duration, []float64) {
	var (
		uploadTimes  []time.Duration
		uploadSpeeds []float64
		data         = make([]byte, fileSize)
	)

	for i := 1; i <= numFiles; i++ {
		rand.Read(data)

		key := fmt.Sprintf("file-%d.dat", i)
		fmt.Printf(" - Trial: %d,", i)
		startTime := time.Now()

		_, err := minioClient.PutObject(context.Background(), bucketName, key, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{})
		if err != nil {
			log.Fatalf(`Unable to upload %s to %s, %v`, key, bucketName, err)
		}

		duration := time.Since(startTime)
		uploadTimes = append(uploadTimes, duration)

		uploadSpeed := float64(fileSize) / duration.Seconds() / 1024 / 1024 // MB/s
		uploadSpeeds = append(uploadSpeeds, uploadSpeed)

		fmt.Printf("\ttime=%s, speed=%.2f MB/s\n", duration, uploadSpeed)
	}

	return uploadTimes, uploadSpeeds
}

func downloadFiles(minioClient *minio.Client, bucketName string, expectedFileSize int64, numFiles int) ([]time.Duration, []float64) {
	var (
		downloadTimes  []time.Duration
		downloadSpeeds []float64
	)

	for i := 1; i <= numFiles; i++ {
		key := fmt.Sprintf("file-%d.dat", i)
		fmt.Printf(" - Trial: %d,", i)
		startTime := time.Now()

		payload, err := minioClient.GetObject(context.Background(), bucketName, key, minio.GetObjectOptions{})
		if err != nil {
			log.Fatalf(`Unable to download %s from %s, %v`, key, bucketName, err)
		}
		payloadSize, err := io.Copy(io.Discard, payload)
		if err != nil {
			log.Fatalf(`Unable to receive %s from %s, %v`, key, bucketName, err)
		}

		if payloadSize != expectedFileSize {
			log.Fatalf(`Unmatched sizes: actual=%d, expected=%d`, payloadSize, expectedFileSize)
		}

		duration := time.Since(startTime)
		downloadTimes = append(downloadTimes, duration)

		downloadSpeed := float64(payloadSize) / duration.Seconds() / 1024 / 1024 // MB/s
		downloadSpeeds = append(downloadSpeeds, downloadSpeed)

		fmt.Printf("\ttime=%s, speed=%.2f MB/s\n", duration, downloadSpeed)
	}

	return downloadTimes, downloadSpeeds
}

func newMinioClient(endpoint, accessKey, secretKey string) (*minio.Client, error) {
	return minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: true,
	})
}
