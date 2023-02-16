package influxdb2robust

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go"
	"github.com/influxdata/influxdb-client-go/api"
)

type writeService struct {
	NewDataCh chan *batch
	ctx       context.Context
	options   *influxdb2.Options
	writeApi  api.WriteApiBlocking
	q         *retryQueue
}

func newWriteService(ctx context.Context, org, bucket, filename string, client influxdb2.Client) (*writeService, error) {
	q, err := newRetryQueue(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open file '%s' for buffer: %s", filename, err)
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

// Run forver, uploading any points from the backlog first, then any new points.
// If there is an error uploading, we change to a state where we shovel new points straight
// into the retry queue for a bit, before resuming uploads.
func (w *writeService) uploadProc() {
	var b *batch

	// When we shutdown, store all the pending data into the database and then close it
	defer func(w *writeService) {
		for {
			bb := <-w.NewDataCh
			if bb != nil {
				w.q.store(bb)
			} else {
				// The writer has closed the channel, we can quit
				break
			}
		}
		w.q.close()
	}(w)

	for {
		// Check whether we are supposed to be shutting down
		if w.ctx.Err() != nil {
			return
		}

		// If we don't already have a batch, see whether there is data in the backlog to upload
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

		if b == nil {
			continue
		}

		// If we get here then we have data to upload, try to upload it
		err := w.writeApi.WriteRecord(w.ctx, b.lines...)
		if err != nil {
			if strings.HasPrefix(err.Error(), "4") {
				// This is hacky, but unfortunately the Error type is now buried in an internal package within
				// influxdb2 so we don't have the status code, just the error string.  If the server rejects the request it will
				// return a 4xx error code, and if we retry this request it will just reject it again and we will be stuck
				// retrying it forever.  So we just log and drop the entire batch.

				// TODO: not sure this actually works?
				log.Printf("Server rejected write, will not retry this batch: %s", err)
				b = nil
				continue
			} else {
				log.Printf("Error writing to influx: %s", err)
				// NB - we do not set b to nil here, so we will try the same batch again next time round the loop.
			}

			if b.id == nil {
				// This data is not already in the retry queue.  Storing it will set an ID.
				w.q.store(b)
			}

			// Wait for the retry interval, but meanwhile store any new datapoints straight into the retry queue
			w.waitForRetry(time.Millisecond * time.Duration(w.options.RetryInterval()))

		} else {
			// Upload was successful
			if b.id != nil {
				w.q.markDone(*b.id)
			}
			b = nil
		}
	}
}

// Wait for the retry interval, but meanwhile store any new datapoints straight into the retry queue
func (w *writeService) waitForRetry(t time.Duration) {
	done := time.After(t)
	for {
		select {
		case <-w.ctx.Done():
			return
		case <-done:
			break
		case bb := <-w.NewDataCh:
			if bb != nil {
				w.q.store(bb)
			} else {
				return
			}
		}
	}
}
