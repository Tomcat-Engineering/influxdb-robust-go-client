package influxdb2robust

import (
	"context"
	"fmt"
	"time"

	"github.com/influxdata/influxdb-client-go"
)

type batch struct {
	lines []string
	id    *uint64
}

type writeService struct {
	NewDataCh chan *batch
	ctx       context.Context
	options   *influxdb2.Options
	writeApi  influxdb2.WriteApiBlocking
	q         *retryQueue
}

func newWriteService(ctx context.Context, org, bucket, filename string, client influxdb2.InfluxDBClient) (*writeService, error) {
	q, err := newRetryQueue(filename)
	if err != nil {
		return nil, fmt.Errorf("Failed to open file '%s' for buffer: %s", filename, err)
	}

	w := writeService{
		NewDataCh: make(chan *batch),
		ctx:       ctx,
		options:   client.Options(),
		writeApi:  client.WriteApiBlocking(org, bucket),
		q:         q,
	}
	go w.uploadProc()
	return &w, nil
}

// Run forver, uploading any points from the backlog first, then
func (w *writeService) uploadProc() {
	defer w.q.close()
	var b *batch
	for {
		// Check that we aren't supposed to be shutting down
		select {
		case <-w.ctx.Done():
			return
		default:
		}

		// See whether there is data in the backlog to upload
		if b == nil {
			b = w.q.first()
		}

		if b == nil {
			// No backlog, wait for new data
			select {
			case b = <-w.NewDataCh:
			case <-w.ctx.Done():
				return
			}
		}

		// If we get here then we have data to upload, try to upload it
		err := w.writeApi.WriteRecord(w.ctx, b.lines...)
		if err != nil {
			if b.id == nil {
				// This data is not already in the retry buffer
				w.q.store(b)
			}

			// Wait for the retry interval, but meanwhile store any new datapoints straight into the database
			t := time.After(time.Millisecond * time.Duration(w.options.RetryInterval()))
			for {
				select {
				case <-w.ctx.Done():
					return
				case <-t:
					break
				case b = <-w.NewDataCh:
					w.q.store(b)
				}
			}
		} else {
			if b.id != nil {
				w.q.markDone(*b.id)
			}
		}
	}
}
