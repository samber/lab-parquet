package main

import (
	"bytes"
	"context"
	"io"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/go-kit/log"
	"github.com/markandrus/s3readerat"
	"github.com/samber/lo"
	"github.com/sirupsen/logrus"
	thanosS3 "github.com/thanos-io/objstore/providers/s3"
)

func download(key string) (io.ReaderAt, error) {
	bucket, err := thanosS3.NewBucketWithConfig(
		log.NewNopLogger(),
		thanosS3.Config{
			Endpoint:  os.Getenv("AWS_ENDPOINT"),
			Insecure:  true,
			Region:    os.Getenv("AWS_REGION"),
			AccessKey: os.Getenv("AWS_ACCESS_KEY"),
			SecretKey: os.Getenv("AWS_SECRET_KEY"),
			Bucket:    os.Getenv("AWS_BUCKET"),
			PartSize:  5 * 1024 * 1024, // 5MB
		},
		"reader",
	)
	if err != nil {
		return nil, err
	}

	defer bucket.Close()

	start := time.Now()

	reader, err := bucket.Get(context.Background(), key)
	if err != nil {
		return nil, err
	}

	defer reader.Close()

	buff, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	logrus.Printf("Download Finished. Time: %s. Size: %dMB.\n", time.Since(start), len(buff)/1024/1024)

	return bytes.NewReader(buff), nil
}

func asyncDownload(key string) (io.ReaderAt, int64, error) {
	cfg, err := config.LoadDefaultConfig(
		context.Background(),
	)
	if err != nil {
		return nil, -1, err
	}

	opts := s3.Options{
		BaseEndpoint: lo.ToPtr("https://" + os.Getenv("AWS_ENDPOINT")),
		Region:       os.Getenv("AWS_REGION"),
		HTTPClient:   cfg.HTTPClient,
		Credentials: credentials.NewStaticCredentialsProvider(
			os.Getenv("AWS_ACCESS_KEY"),
			os.Getenv("AWS_SECRET_KEY"),
			"",
		),
		APIOptions:    cfg.APIOptions,
		Logger:        cfg.Logger,
		ClientLogMode: cfg.ClientLogMode,
	}

	reader, err := s3readerat.NewWithOptions(s3readerat.Options{
		Options: &opts,
		Bucket:  os.Getenv("AWS_BUCKET"),
		Key:     key,
	})
	if err != nil {
		return nil, -1, err
	}

	size, err := reader.Size()
	if err != nil {
		return nil, -1, err
	}

	return reader, size, nil
}
