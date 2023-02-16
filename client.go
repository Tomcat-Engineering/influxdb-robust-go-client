// Package influxdb2robust provides a variant of the standard influxdb2 go client
// library (https://github.com/influxdata/influxdb-client-go) which buffers new
// data in a local BoltDB database so that we can safely collect data and upload it
// to a remote InfluxDB v2 instance despite a bad network connection.
package influxdb2robust

import (
	influxdb2 "github.com/influxdata/influxdb-client-go"
	"github.com/influxdata/influxdb-client-go/api"
)

// InfluxDBRobustClient implements the API to communicate with an InfluxDBServer
// There two APIs for writing, WriteAPI and WriteAPIBlocking.
// WriteAPI provides asynchronous, non-blocking, methods for writing time series data,
// and uses the robust Boltdb buffer.
// WriteAPIBlocking provides blocking methods for writing time series data and does not
// use the buffer.
type InfluxDBRobustClient struct {
	influxdb2.Client
	writeApis []api.WriteAPI
}

// NewClientWithOptions creates Client for connecting to given serverUrl with provided authentication token
// and configured with custom Options
// Authentication token can be empty in case of connecting to newly installed InfluxDB server, which has not been set up yet.
// In such case Setup will set authentication token
func NewClientWithOptions(serverUrl string, authToken string, options *influxdb2.Options) *InfluxDBRobustClient {
	// Disable retries for the underlying client, otherwise it will use its own in-memory buffer
	// to store failed uploads - we want to store them in our on-disk buffer instead.
	options.SetMaxRetries(0)
	return &InfluxDBRobustClient{
		Client: influxdb2.NewClientWithOptions(serverUrl, authToken, options),
	}
}

// NewClient creates Client for connecting to given serverUrl with provided authentication token, with the default options.
// Authentication token can be empty in case of connecting to newly installed InfluxDB server, which has not been set up yet.
// In such case Setup will set authentication token
func NewClient(serverUrl string, authToken string) *InfluxDBRobustClient {
	return NewClientWithOptions(serverUrl, authToken, influxdb2.DefaultOptions())
}

// Close ensures all ongoing asynchronous write clients finish
func (c *InfluxDBRobustClient) Close() {
	for _, w := range c.writeApis {
		w.Close()
	}
	c.Client.Close()
}

// WriteAPI returns the asynchronous, non-blocking, Write client.
// This is the only method which is implemented differently in the "robust" version.
// Note the extra `filename` argument, and that it can return an error.
func (c *InfluxDBRobustClient) WriteAPI(org, bucket, filename string) (api.WriteAPI, error) {
	w, err := newWriter(org, bucket, filename, c.Client)
	if err == nil {
		c.writeApis = append(c.writeApis, w)
	}
	return w, err
}

// WriteApi returns the asynchronous, non-blocking, Write client.
// This is the only method which is implemented differently in the "robust" version.
// Note the extra `filename` argument, and that it can return an error.
// Deprecated version, use WriteAPI instead.
func (c *InfluxDBRobustClient) WriteApi(org, bucket, filename string) (api.WriteApi, error) {
	w, err := newWriter(org, bucket, filename, c.Client)
	if err == nil {
		c.writeApis = append(c.writeApis, w)
	}
	return w, err
}

// The methods below are just proxied so that we can add a doc comment noting that
// they are not buffered.

// WriteAPIBlocking returns the synchronous, blocking, Write client.
// We allow direct access to the underlying blocking client - blocking writes will tell the caller
// that the write failed, so we don't need the magic persistent buffer.
func (c *InfluxDBRobustClient) WriteAPIBlocking(org, bucket string) api.WriteAPIBlocking {
	return c.Client.WriteAPIBlocking(org, bucket)
}

// WriteApiBlocking returns the synchronous, blocking, Write client.
// We allow direct access to the underlying blocking client - blocking writes will tell the caller
// that the write failed, so we don't need the magic persistent buffer.
// This is deprecated, use WriteAPIBlocking instead.
func (c *InfluxDBRobustClient) WriteApiBlocking(org, bucket string) api.WriteApiBlocking {
	return c.Client.WriteApiBlocking(org, bucket)
}
