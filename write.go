package main

import (
	"bytes"
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
	org string
	bucket string
	options influxdb2.Options
	batchSize int
	db *datastore
	stopCh chan bool
	writeBuffer []*PtWithMeta
}

func NewWriter(client influxdb2.InfluxDBClient, db *datastore, org, bucket string, options *influxdb2.Options) *writer {
	w := writer{
		org: org, 
		bucket: bucket, 
		options: *options,
		db: db,
		stopCh: make(chan bool),
		writeBuffer: make([]*PtWithMeta, 0, options.BatchSize()+1),
	}
	go w.run(client)
	return &w
}

func (w *writer) WriteRecord(line string) {
	w.db.In <- &PtWithMeta{Org: w.org, Bucket: w.bucket, Line: line}
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
}

func (w *writer) Close() {
	w.stopCh <- true
}

func (w *writer) Errors() <-chan error {
	return nil
}

// Background process which gets any old data from the database and uploads it, then
// listens for new data and uploads that.  It batches stuff, using code stolen from
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

		case <-w.stopCh:
			ticker.Stop()
			w.db.CloseNewDataChannel(inputCh)
			return
		}
	}
	
}

func (w *writer) flushBuffer(baseWriter influxdb2.WriteApiBlocking) {
	if len(w.writeBuffer) > 0 {
		var lines []string
		for _, pt := range(w.writeBuffer) {
			lines = append(lines, pt.Line)
		}

		// Attempt to upload the data
		err := baseWriter.WriteRecord(nil, lines...)

		if err != nil {
			// TODO: wait

		} else {
			// Mark stuff as done.  This is only called from run(), therefore
			// nothing can have been added to writeBuffer since we created `lines`.
			for _, pt := range(w.writeBuffer) {
				w.db.Done <- pt
			}
			w.writeBuffer = w.writeBuffer[:0]
		}
	}	
}


