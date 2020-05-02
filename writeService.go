package influxdb2robust

import (
	"context"
	"log"

	"github.com/influxdata/influxdb-client-go"
)

type writeService struct {
	ctx       context.Context
	NewDataCh chan string
}

func newWriteService(ctx context.Context, org, bucket, filename string, client influxdb2.InfluxDBClient) (*writeService, error) {
	w := writeService{
		ctx:       ctx,
		NewDataCh: make(chan string),
	}
	go w.uploadProc()
	return &w, nil
}

func (w *writeService) uploadProc() {
	for {
		select {
		case batch := <-w.NewDataCh:
			log.Printf("Got new data %s\n", batch)
		case <-w.ctx.Done():
			return
		}
	}
}
