package influxdb2robust

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/influxdata/influxdb-client-go"
	bolt "go.etcd.io/bbolt"
)

type batch struct {
	lines []string
}

type writeService struct {
	NewDataCh chan *batch
	ctx       context.Context
	client    influxdb2.InfluxDBClient
	db        *bolt.DB
}

func newWriteService(ctx context.Context, org, bucket, filename string, client influxdb2.InfluxDBClient) (*writeService, error) {
	db, err := bolt.Open(filename, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("Failed to open database: %s", err)
	}

	w := writeService{
		NewDataCh: make(chan *batch),
		ctx:       ctx,
		client:  client,
		db:        db,
	}
	go w.uploadProc()
	return &w, nil
}

func (w *writeService) uploadProc() {
	defer w.db.Close()
	var b *batch
	for {
		select {
		case b = <-w.NewDataCh:
			log.Printf("Got new data %s\n", b)
		case <-w.ctx.Done():
			return
		}

		
	}
}

