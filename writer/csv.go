package main

import (
	"os"
	"time"

	"github.com/gocarina/gocsv"
	"github.com/sirupsen/logrus"
)

type csvRow struct {
	Region       string    `csv:"region"`
	Variable     string    `csv:"variable"`
	Attribute    string    `csv:"attribute"`
	UTCTimestamp time.Time `csv:"utc_timestamp"`
	Value        float32   `csv:"data"`
}

func readCSV(path string) ([]csvRow, error) {
	start := time.Now()

	file, err := os.Open(path)
	if err != nil {
		return []csvRow{}, err
	}

	rows := []csvRow{}

	err = gocsv.UnmarshalFile(file, &rows)
	if err != nil {
		return []csvRow{}, err
	}

	logrus.Println("CSV reading Finished. Time:", time.Since(start))

	return rows, nil
}
