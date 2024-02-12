// This file is a part of `github.com/thekondor/s3-simple-benchmarker`
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"flag"
	"fmt"
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
		fmt.Printf("Usage: %s -endpoint <S3 endpoint> -accessKey <S3 access key> -secretKey <S3 secret key> -bucketName <S3 bucket name>\n", os.Args[0])
		fmt.Printf("\nEnvironment variables (override):\n")
		fmt.Printf("- $%s\n", accessKeyEnvVarName)
		fmt.Printf("- $%s\n", secretKeyEnvVarName)
		os.Exit(1)
	}

	fileSizeMb *= 1024 * 1024

	minioClient, err := newMinioClient(endpoint, accessKey, secretKey)
	if err != nil {
		log.Fatalf(`Error creating MinIO client: %v`, err)
	}

	log.Printf(`Upload:`)
	uploadTimes, uploadSpeeds := uploadFiles(minioClient, bucketName, fileSizeMb, trials)
	log.Printf(`Download:`)
	downloadTimes, downloadSpeeds := downloadFiles(minioClient, bucketName, fileSizeMb, trials)

	displayResults("Upload", uploadTimes, uploadSpeeds)
	displayResults("Download", downloadTimes, downloadSpeeds)
}

func displayResults(operation string, times []time.Duration, speeds []float64) {
	averageTime := calculateAverage(times)
	fmt.Printf("\nAVG %s Time: %v\n", operation, averageTime)

	p90Time := calculateP90(times)
	fmt.Printf("p90 %s Time: %v\n", operation, p90Time)

	p90Speed := calculateP90(speeds)
	fmt.Printf("p90 %s Speed: %.2f MB/s\n", operation, p90Speed)
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
		log.Printf(` - Trial: %d`, i)
		startTime := time.Now()

		_, err := minioClient.PutObject(context.Background(), bucketName, key, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{})
		if err != nil {
			log.Fatalf(`Unable to upload %s to %s, %v`, key, bucketName, err)
		}

		duration := time.Since(startTime)
		uploadTimes = append(uploadTimes, duration)

		uploadSpeed := float64(fileSize) / duration.Seconds() / 1024 / 1024 // MB/s
		uploadSpeeds = append(uploadSpeeds, uploadSpeed)

		log.Printf(`   duration=%s`, duration)
	}

	return uploadTimes, uploadSpeeds
}

func downloadFiles(minioClient *minio.Client, bucketName string, fileSize, numFiles int) ([]time.Duration, []float64) {
	var (
		downloadTimes  []time.Duration
		downloadSpeeds []float64
	)

	for i := 1; i <= numFiles; i++ {
		key := fmt.Sprintf("file-%d.dat", i)
		log.Printf(` - Trial: %d`, i)
		startTime := time.Now()

		payload, err := minioClient.GetObject(context.Background(), bucketName, key, minio.GetObjectOptions{})
		if err != nil {
			log.Fatalf(`Unable to download %s from %s, %v`, key, bucketName, err)
		}
		payloadStat, err := payload.Stat()
		if err != nil {
			log.Fatalf(`Unable to get stat for %s from %s, %v`, key, bucketName, err)
		}

		duration := time.Since(startTime)
		downloadTimes = append(downloadTimes, duration)

		downloadSpeed := float64(fileSize) / duration.Seconds() / 1024 / 1024 // MB/s
		downloadSpeeds = append(downloadSpeeds, downloadSpeed)

		log.Printf(`   size=%d, duration=%s`, payloadStat.Size, duration)
	}

	return downloadTimes, downloadSpeeds
}

func newMinioClient(endpoint, accessKey, secretKey string) (*minio.Client, error) {
	return minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: true,
	})
}
