package main

import (
	"bufio"
	csvreader "encoding/csv"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/gitdev1234/CSV2Influx/csv"
)

var (
	host     string
	database string
	username string
	password string

	tag string

	path   string
	header bool

	influx *InfluxConnection
)

func main() {
	flag.StringVar(&host, "h", "http://localhost:8086", "host of influxdb")
	flag.StringVar(&database, "db", "WeatherData", "database of influxdb")
	flag.StringVar(&username, "username", "", "username of influxdb")
	flag.StringVar(&password, "password", "", "password of influxdb")

	flag.StringVar(&tag, "tag", "WeatherStation", "DataSource tag")

	flag.StringVar(&path, "f", "test/*.csv", "files to import")
	flag.BoolVar(&header, "header", true, "csv has a header line")
	flag.Parse()

	files, _ := filepath.Glob(path)
	lines := &csv.MergedLines{}
	for _, file := range files {
		openfile, _ := os.Open(file)
		defer openfile.Close()
		r := csvreader.NewReader(bufio.NewReader(openfile))
		tmpheader := header
		for {
			record, err := r.Read()
			// Stop at EOF.
			if err == io.EOF {
				break
			}
			if !tmpheader {
				lines.Add(record)
			} else {
				tmpheader = false
			}
		}
		fmt.Printf("imported %v\n", file)
	}
	for time, values := range lines.LinesTime {
		fmt.Printf("%v: %v\n", time, values)
	}
	fmt.Println("write to influxdb database")
	influx = NewInfluxConnection()
	influx.AddMultiline(lines)

	if influx != nil {
		influx.Close()
	}
}
