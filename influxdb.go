package main

import (
	"log"
	"sync"
	"time"

	"github.com/influxdata/influxdb/client/v2"

	"github.com/gitdev1234/CSV2Influx/csv"
)

const batchDuration time.Duration = time.Duration(5000)

type InfluxConnection struct {
	points chan *client.Point
	wg     sync.WaitGroup
	client client.Client
}

func NewInfluxConnection() *InfluxConnection {
	// Make client
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     host,
		Username: username,
		Password: password,
	})

	if err != nil {
		panic(err)
	}

	db := &InfluxConnection{
		client: c,
		points: make(chan *client.Point, 500),
	}

	// start worker
	db.wg.Add(1)
	go db.worker()

	return db
}

func (c *InfluxConnection) Close() {
	close(c.points)
	c.wg.Wait()
	c.client.Close()
}

// stores data points in batches into the influxdb
func (c *InfluxConnection) worker() {
	bpConfig := client.BatchPointsConfig{
		Database:  database,
		Precision: "m",
	}

	var bp client.BatchPoints
	var err error
	var writeNow, closed bool
	timer := time.NewTimer(batchDuration)

	for !closed {

		// wait for new points
		select {
		case point, ok := <-c.points:
			if ok {
				if bp == nil {
					// create new batch
					timer.Reset(batchDuration)
					if bp, err = client.NewBatchPoints(bpConfig); err != nil {
						log.Fatal(err)
					}
				}
				bp.AddPoint(point)
			} else {
				closed = true
			}
		case <-timer.C:
			if bp == nil {
				timer.Reset(batchDuration)
			} else {
				writeNow = true
			}
		}

		// write batch now?
		if bp != nil && (writeNow || closed) {
			log.Println("saving", len(bp.Points()), "points")

			if err = c.client.Write(bp); err != nil {
				log.Fatal(err)
			}
			writeNow = false
			bp = nil
		}
	}

	timer.Stop()
	c.wg.Done()
}

func (c *InfluxConnection) AddMultiline(lines *csv.MergedLines) {
	tags := map[string]string{
		"DataSource": tag,
	}
	for timestamp, fields := range lines.LinesTime {
		point, err := client.NewPoint("point", tags, fields, timestamp)
		if err != nil {
			panic(err)
		}
		c.points <- point
	}
}
