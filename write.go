package influxdb2robust

import (
	"bytes"
	"context"
	"log"
	"time"

	"github.com/influxdata/influxdb-client-go"
	lp "github.com/influxdata/line-protocol"
)

// Create something that implements the influxdb2.WriteApi interface,
// but actually just pushes the data into our boltdb.  The interface is:

/*
type WriteApi interface {
	// WriteRecord writes asynchronously line protocol record into bucket.
	// WriteRecord adds record into the buffer which is sent on the background when it reaches the batch size.
	// Blocking alternative is available in the WriteApiBlocking interface
	WriteRecord(line string)
	// WritePoint writes asynchronously Point into bucket.
	// WritePoint adds Point into the buffer which is sent on the background when it reaches the batch size.
	// Blocking alternative is available in the WriteApiBlocking interface
	WritePoint(point *Point)
	// Flush forces all pending writes from the buffer to be sent
	Flush()
	// Flushes all pending writes and stop async processes. After this the Write client cannot be used
	Close()
	// Errors return channel for reading errors which occurs during async writes
	Errors() <-chan error
}
*/

type writer struct {
	org         string
	bucket      string
	options     influxdb2.Options
	batchSize   int
	db          *datastore
	ctx         context.Context
	cancel      context.CancelFunc
	flushCh     chan bool // signals that we should upload the current batch of points immediately
	doneCh      chan bool // signals that we have shut down
	errCh       chan error
	writeBuffer []*ptWithMeta
}

func newWriter(client influxdb2.InfluxDBClient, db *datastore, org, bucket string, options *influxdb2.Options) *writer {
	ctx, cancelFunc := context.WithCancel(context.Background())
	w := writer{
		org:         org,
		bucket:      bucket,
		options:     *options,
		db:          db,
		ctx:         ctx,
		cancel:      cancelFunc,
		flushCh:     make(chan bool),
		doneCh:      make(chan bool),
		writeBuffer: make([]*ptWithMeta, 0, options.BatchSize()+1),
	}
	go w.run(client)
	return &w
}

func (w *writer) WriteRecord(line string) {
	w.db.In <- &ptWithMeta{Org: w.org, Bucket: w.bucket, Line: line}
}

func (w *writer) WritePoint(point *influxdb2.Point) {
	// Convert to line-protocol record
	var buffer bytes.Buffer
	e := lp.NewEncoder(&buffer)
	e.SetFieldTypeSupport(lp.UintSupport)
	e.FailOnFieldErr(true)
	e.SetPrecision(w.options.Precision())
	_, err := e.Encode(point)
	if err != nil {
		log.Printf("point encoding error: %s\n", err.Error())
		return
	}
	w.WriteRecord(buffer.String())
}

func (w *writer) Flush() {
	w.flushCh <- true
}

func (w *writer) Close() {
	w.cancel()
	<-w.doneCh
	if w.errCh != nil {
		close(w.errCh)
		w.errCh = nil
	}
}

func (w *writer) Errors() <-chan error {
	if w.errCh == nil {
		w.errCh = make(chan error)
	}
	return w.errCh
}

// Backlog tells you how many messages are queued for this client.
// This is not part of the influxdb2.WriteApi interface, so external code can't actually use it yet.
func (w *writer) Backlog() uint64 {
	return w.db.Backlog(w.org, w.bucket)
}

// Background process which gets any old data from the database and uploads it, then
// listens for new data and uploads that.  It batches points, using code stolen from
// the non-blocking influx2 client.
func (w *writer) run(client influxdb2.InfluxDBClient) {
	baseWriter := client.WriteApiBlocking(w.org, w.bucket)
	inputCh := w.db.GetNewDataChannel(w.org, w.bucket)
	ticker := time.NewTicker(time.Duration(w.options.FlushInterval()) * time.Millisecond)

	for {
		select {
		case pt := <-inputCh:
			w.writeBuffer = append(w.writeBuffer, pt)
			if len(w.writeBuffer) >= int(w.options.BatchSize()) {
				w.flushBuffer(baseWriter)
			}

		case <-ticker.C:
			w.flushBuffer(baseWriter)

		case <-w.flushCh:
			w.flushBuffer(baseWriter)

		case <-w.ctx.Done():
			ticker.Stop()
			w.db.CloseNewDataChannel(inputCh)
			w.doneCh <- true
			return
		}
	}

}

func (w *writer) flushBuffer(baseWriter influxdb2.WriteApiBlocking) {
	if len(w.writeBuffer) > 0 {
		var lines []string
		for _, pt := range w.writeBuffer {
			lines = append(lines, pt.Line)
		}

		// Attempt to upload the data
		err := baseWriter.WriteRecord(w.ctx, lines...)

		if err != nil {
			// Tell anyone listening about the error
			if w.errCh != nil {
				w.errCh <- err
			}

			// Wait for a bit.  This goroutine is only doing uploads, so if
			// the server connection is broken we should just wait.
			// Default Influxdb RetryInterval is 1 second!
			select {
			case <-time.After(time.Millisecond * time.Duration(w.options.RetryInterval())):
			case <-w.ctx.Done():
			}

		} else {
			// Mark stuff as done.  This is only called from run(), therefore
			// nothing can have been added to writeBuffer since we created `lines`.
			for _, pt := range w.writeBuffer {
				w.db.Done <- pt
			}
			w.writeBuffer = w.writeBuffer[:0]
		}
	}
}
