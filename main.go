package main

import (
	"bufio"
	csvreader "encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/gitdev1234/CSV2Influx/csv"
)

var (
	host     string
	tag      string
	path     string
	database string
	influx   *InfluxConnection
)

func main() {
	flag.StringVar(&host, "h", "localhost", "host of influxdb (default: localhost)")
	flag.StringVar(&tag, "tag", "WeatherStation", "DataSource tag (default: WeatherStation)")
	flag.StringVar(&path, "f", "test/*.csv", "files to import (default: test/*.csv)")
	flag.Parse()

	files, _ := filepath.Glob(path)
	lines := &csv.MergedLines{}
	for _, file := range files {
		openfile, _ := os.Open(file)
		defer openfile.Close()
		r := csvreader.NewReader(bufio.NewReader(openfile))
		for {
			record, err := r.Read()
			// Stop at EOF.
			if err == io.EOF {
				break
			}
			fmt.Printf("  %v: %v : %v \n", record[0], record[1], record[2])
		}
	}

	influx = NewInfluxConnection()
	influx.AddMultiline(lines)
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigs
	log.Println("received", sig)

	if influx != nil {
		influx.Close()
	}
}
