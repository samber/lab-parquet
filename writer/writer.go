package main

import (
	"bytes"
	"fmt"
	"math"
	"sort"
	"time"

	parquet "github.com/parquet-go/parquet-go"
	"github.com/sirupsen/logrus"
)

//
// Used by github.com/xitongsys/parquet-go
//
//	type Item struct {
//		Region       string  `parquet:"name=region, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
//		Variable     string  `parquet:"name=variable, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
//		Attribute    string  `parquet:"name=attribute, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
//		UTCTimestamp int32   `parquet:"name=utc_timestamp, type=INT32, convertedtype=TIME_MILLIS"`
//		Value        float32 `parquet:"name=value, type=FLOAT"`
//	}

type Item struct {
	Region       string    `parquet:"region"`
	Variable     string    `parquet:"variable"`
	Attribute    string    `parquet:"attribute"`
	UTCTimestamp time.Time `parquet:"utc_timestamp"`
	Value        float32   `parquet:"value"`
}

func main() {
	rows, err := readCSV("../input/time_series_60min_stacked.csv")
	if err != nil {
		logrus.Fatal(err.Error())
	}

	total := len(rows)
	start := time.Now()

	fmt.Println("Total rows:", total)

	schema := parquet.SchemaOf(new(Item))
	buffer := parquet.NewGenericBuffer[Item](
		schema,
		parquet.SortingRowGroupConfig(
			parquet.SortingColumns(
				parquet.Ascending("attribute"),
				parquet.Ascending("variable"),
				parquet.Ascending("region"),
				parquet.Descending("utc_timestamp"),
			),
		),
	)

	for i, row := range rows {
		_, err = buffer.Write([]Item{
			{
				Region:       row.Region,
				Variable:     row.Variable,
				Attribute:    row.Attribute,
				UTCTimestamp: row.UTCTimestamp,
				Value:        row.Value,
			},
		})
		if err != nil {
			logrus.Fatal(err.Error())
		}

		if i%100000 == 0 {
			fmt.Printf("Items: %d/%d (%d%%)\n", i, total, int(math.Round(float64(i)/float64(total)*100)))
		}
	}

	sort.Sort(buffer)

	// output, err := os.Create("../output/entsoe.parquet")
	// if err != nil {
	// 	logrus.Fatal(err.Error())
	// }
	// defer output.Close()

	b := bytes.NewBuffer(nil)
	writer := parquet.NewGenericWriter[Item](
		b,
		schema,
		parquet.Compression(&parquet.Lz4Raw),
		parquet.MaxRowsPerRowGroup(1*1024*1024),
		parquet.BloomFilters(
			parquet.SplitBlockFilter(4, "attribute"),
			parquet.SplitBlockFilter(4, "variable"),
			parquet.SplitBlockFilter(6, "region"),
		),
	)

	_, err = parquet.CopyRows(writer, buffer.Rows())
	if err != nil {
		logrus.Fatal(err.Error())
	}

	err = writer.Close()
	if err != nil {
		logrus.Fatal(err.Error())
	}

	err = writer.Flush()
	if err != nil {
		logrus.Fatal(err.Error())
	}

	logrus.Println("Parquet file built. Time:", time.Since(start))

	err = upload("parquet/entsoe.parquet", b)
	if err != nil {
		logrus.Fatal(err.Error())
	}
}
