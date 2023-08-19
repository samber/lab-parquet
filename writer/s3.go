package main

import (
	"context"
	"io"
	"os"
	"time"

	"github.com/go-kit/log"
	"github.com/samber/go-metered-io"
	"github.com/sirupsen/logrus"
	"github.com/thanos-io/objstore/providers/s3"
)

// @TODO: should be io.ReaderAt for multipart upload
func upload(key string, reader io.Reader) error {
	bucket, err := s3.NewBucketWithConfig(
		log.NewNopLogger(),
		s3.Config{
			Endpoint:  os.Getenv("AWS_ENDPOINT"),
			Insecure:  true,
			Region:    os.Getenv("AWS_REGION"),
			AccessKey: os.Getenv("AWS_ACCESS_KEY"),
			SecretKey: os.Getenv("AWS_SECRET_KEY"),
			Bucket:    os.Getenv("AWS_BUCKET"),
			PartSize:  5 * 1024 * 1024, // 5MB
		},
		"writer",
	)
	if err != nil {
		return err
	}

	defer bucket.Close()

	meteredReader := metered.NewReader(reader)

	start := time.Now()
	defer func() {
		logrus.Printf("Upload Finished. Time: %s. Size: %dMB.\n", time.Since(start), meteredReader.Rx()/1024/1024)
	}()

	return bucket.Upload(context.Background(), key, meteredReader)
}
