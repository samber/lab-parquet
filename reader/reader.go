package main

import (
	"fmt"
	"log"
	"math"
	"time"

	parquet "github.com/parquet-go/parquet-go"
	"github.com/sirupsen/logrus"
)

//
// Used by github.com/xitongsys/parquet-go
//
// type Item struct {
// 	Region       string  `parquet:"name=region, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
// 	Variable     string  `parquet:"name=variable, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
// 	Attribute    string  `parquet:"name=attribute, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
// 	UTCTimestamp int32   `parquet:"name=utc_timestamp, type=INT32, convertedtype=TIME_MILLIS"`
// 	Value        float32 `parquet:"name=value, type=FLOAT"`
// }

type Item struct {
	Region       string    `parquet:"region"`
	Variable     string    `parquet:"variable"`
	Attribute    string    `parquet:"attribute"`
	UTCTimestamp time.Time `parquet:"utc_timestamp"`
	Value        float32   `parquet:"value"`
}

func (i Item) String() string {
	return fmt.Sprintf("Region: %s, Variable: %s, Attribute: %s, UTCTimestamp: %s, Value: %f", i.Region, i.Variable, i.Attribute, i.UTCTimestamp, i.Value)
}

const parquetObjectKey = "parquet/entsoe.parquet"

func main() {
	// input, err := os.Open("../output/entsoe.parquet")
	// if err != nil {
	// 	logrus.Fatal(err.Error())
	// }
	// defer input.Close()

	// input, err := download("parquet/entsoe.parquet")
	// if err != nil {
	// 	logrus.Fatal(err.Error())
	// }

	fullScan()
	// countFr()
}

func fullScan() {
	input, err := download(parquetObjectKey)
	if err != nil {
		logrus.Fatal(err.Error())
	}

	reader := parquet.NewGenericReader[Item](input)
	defer reader.Close()

	start := time.Now()

	total := int(reader.NumRows())

	fmt.Println("Total rows:", total)

	for i := 0; i < total; i++ {
		items := make([]Item, 1)
		_, err := reader.Read(items)
		if err != nil {
			log.Println("Read error", err)
		}

		logrus.Println(items)

		if i%100000 == 0 {
			fmt.Printf("Items: %d/%d (%d%%)\n", i, total, int(math.Round(float64(i)/float64(total)*100)))
		}
	}

	logrus.Println("Read Finished. Time:", time.Since(start))
}

func countFr() {
	input, size, err := asyncDownload(parquetObjectKey)
	if err != nil {
		logrus.Fatal(err.Error())
	}

	reader, err := parquet.OpenFile(input, size, parquet.FileReadMode(parquet.ReadModeAsync))
	if err != nil {
		logrus.Fatal(err.Error())
	}

	regionColumn, ok := reader.Schema().Lookup("region")
	if !ok {
		logrus.Fatal("missing region column")
	}

	var candidateChunks []parquet.ColumnChunk

	for _, rowGroup := range reader.RowGroups() {
		columnChunk := rowGroup.ColumnChunks()[regionColumn.ColumnIndex]
		bloomFilter := columnChunk.BloomFilter()

		if bloomFilter != nil {
			ok, err := bloomFilter.Check(parquet.ValueOf("FR"))
			if err != nil {
				logrus.Fatal(err.Error())
			}
			if !ok {
				continue
			}
		}

		candidateChunks = append(candidateChunks, columnChunk)
	}

	countRowGroups := len(reader.RowGroups())
	countCandidateChunks := len(candidateChunks)

	fmt.Printf("%d row group and %d chunks containing region=\"FR\"", countRowGroups, countCandidateChunks)
}
