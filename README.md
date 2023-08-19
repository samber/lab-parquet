
## Tuto

```sh
wget https://data.open-power-system-data.org/time_series/2020-10-06/time_series_60min_stacked.csv

head -n 1000000 data/time_series_60min_stacked.csv > data/time_series_60min_stacked.csv.1M
head -n 100000 data/time_series_60min_stacked.csv > data/time_series_60min_stacked.csv.100k
head -n 10000 data/time_series_60min_stacked.csv > data/time_series_60min_stacked.csv.10k
```

```sh
docker-compose up -d

export AWS_ENDPOINT=localhost:9000
export AWS_REGION=
export AWS_ACCESS_KEY=helloworld
export AWS_SECRET_KEY=helloworld
export AWS_BUCKET=test

go run writer/*.go
go run reader/*.go
```
