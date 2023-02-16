package influxdb2robust

import (
	"bytes"
	"context"
	"log"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go"
	writeapi "github.com/influxdata/influxdb-client-go/api/write"
	lp "github.com/influxdata/line-protocol"
)

// Create something that implements the influxdb2.api.WriteAPI interface but uses the
// boltdb-backed queue rather than just an in-memory one.

// The "writer" basically just deals with batching up incoming points and then
// passes the data to the writeService to actually upload it.

// This code is mostly copied from the original influxdb2 client.

type writer struct {
	service     *writeService
	writeBuffer []string
	ctx         context.Context
	cancelFunc  context.CancelFunc
	options     *influxdb2.Options
	bufferCh    chan string
	errCh       chan error
	doneCh      chan struct{}
	flushCh     chan int
}

func newWriter(org, bucket, filename string, client influxdb2.Client) (*writer, error) {
	w := writer{
		options:  client.Options(),
		bufferCh: make(chan string),
		errCh:    make(chan error),
		doneCh:   make(chan struct{}),
		flushCh:  make(chan int),
	}
	w.ctx, w.cancelFunc = context.WithCancel(context.Background())
	var err error
	w.service, err = newWriteService(w.ctx, org, bucket, filename, client)
	if err != nil {
		return nil, err
	}
	go w.bufferProc()
	return &w, nil
}

func (w *writer) WriteRecord(line string) {
	b := []byte(line)
	b = append(b, 0xa)
	w.bufferCh <- string(b)
}

func (w *writer) WritePoint(point *writeapi.Point) {
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
	w.flushCh <- 1
	// TODO - wait for flushing?
}

func (w *writer) Close() {
	w.cancelFunc()
	if w.errCh == nil {
		close(w.errCh)
	}
	<-w.doneCh
}

func (w *writer) Errors() <-chan error {
	if w.errCh == nil {
		w.errCh = make(chan error)
	}
	return w.errCh
}

func (w *writer) bufferProc() {
	ticker := time.NewTicker(time.Duration(w.options.FlushInterval()) * time.Millisecond)
x:
	for {
		select {
		case line := <-w.bufferCh:
			w.writeBuffer = append(w.writeBuffer, line)
			if len(w.writeBuffer) == int(w.options.BatchSize()) {
				w.flushBuffer()
			}

		case <-ticker.C:
			w.flushBuffer()

		case <-w.flushCh:
			w.flushBuffer()

		case <-w.ctx.Done():
			ticker.Stop()
			w.flushBuffer()
			close(w.service.NewDataCh)
			break x
		}
	}
	close(w.doneCh)
}

func (w *writer) flushBuffer() {
	if len(w.writeBuffer) > 0 {
		w.service.NewDataCh <- &batch{lines: w.writeBuffer}
		w.writeBuffer = w.writeBuffer[:0]
	}
}
