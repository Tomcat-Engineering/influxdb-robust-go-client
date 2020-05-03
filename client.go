// Package influxdb2robust provides a variant of the standard influxdb2 go client
// library (https://github.com/influxdata/influxdb-client-go) which buffers new
// data in a local BoltDB database so that we can safely collect data and upload it
// to a remote InfluxDB v2 instance despite a bad network connection.
package influxdb2robust

import (
	"context"

	"github.com/influxdata/influxdb-client-go"
	"github.com/influxdata/influxdb-client-go/api"
	"github.com/influxdata/influxdb-client-go/domain"
)

// InfluxDBRobustClient implements the API to communicate with an InfluxDBServer
// There two APIs for writing, WriteApi and WriteApiBlocking.
// WriteApi provides asynchronous, non-blocking, methods for writing time series data,
// and uses the robust Boltdb buffer.
// WriteApiBlocking provides blocking methods for writing time series data and does not
// use the buffer.
type InfluxDBRobustClient struct {
	BaseClient influxdb2.InfluxDBClient
	writeApis  []influxdb2.WriteApi
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
		BaseClient: influxdb2.NewClientWithOptions(serverUrl, authToken, options),
	}
}

// NewClient creates Client for connecting to given serverUrl with provided authentication token, with the default options.
// Authentication token can be empty in case of connecting to newly installed InfluxDB server, which has not been set up yet.
// In such case Setup will set authentication token
func NewClient(serverUrl string, authToken string) *InfluxDBRobustClient {
	return NewClientWithOptions(serverUrl, authToken, influxdb2.DefaultOptions())
}

// Delegate most things to the normal client that we are wrapping

// Options returns the options associated with client
func (c *InfluxDBRobustClient) Options() *influxdb2.Options {
	return c.BaseClient.Options()
}

// ServerUrl returns the url of the server url client talks to
func (c *InfluxDBRobustClient) ServerUrl() string {
	return c.BaseClient.ServerUrl()
}

// Close ensures all ongoing asynchronous write clients finish
func (c *InfluxDBRobustClient) Close() {
	for _, w := range c.writeApis {
		w.Close()
	}
	c.BaseClient.Close()
}

// Setup sends request to initialise new InfluxDB server with user, org and bucket, and data retention period
// Retention period of zero will result to infinite retention
// and returns details about newly created entities along with the authorization object
func (c *InfluxDBRobustClient) Setup(ctx context.Context, username, password, org, bucket string, retentionPeriodHours int) (*domain.OnboardingResponse, error) {
	return c.BaseClient.Setup(ctx, username, password, org, bucket, retentionPeriodHours)
}

// Ready checks InfluxDB server is running
func (c *InfluxDBRobustClient) Ready(ctx context.Context) (bool, error) {
	return c.BaseClient.Ready(ctx)
}

// QueryApi returns Query client
func (c *InfluxDBRobustClient) QueryApi(org string) influxdb2.QueryApi {
	return c.BaseClient.QueryApi(org)
}

// AuthorizationsApi returns Authorizations API client
func (c *InfluxDBRobustClient) AuthorizationsApi() api.AuthorizationsApi {
	return c.BaseClient.AuthorizationsApi()
}

// OrganizationsApi returns Organizations API client
func (c *InfluxDBRobustClient) OrganizationsApi() api.OrganizationsApi {
	return c.BaseClient.OrganizationsApi()
}

// UsersApi returns Users API client
func (c *InfluxDBRobustClient) UsersApi() api.UsersApi {
	return c.BaseClient.UsersApi()
}

// WriteApi returns the asynchronous, non-blocking, Write client.
// This is the only method which is implemented differently in the "robust" version.
// Note the extra `filename` argument, and that it can return an error.
func (c *InfluxDBRobustClient) WriteApi(org, bucket, filename string) (influxdb2.WriteApi, error) {
	w, err := newWriter(org, bucket, filename, c.BaseClient)
	if err == nil {
		c.writeApis = append(c.writeApis, w)
	}
	return w, err
}

// WriteApiBlocking returns the synchronous, blocking, Write client.
// We allow direct access to the underlying blocking client - blocking writes will tell the caller
// that the write failed, so we don't need the magic persistent buffer.
func (c *InfluxDBRobustClient) WriteApiBlocking(org, bucket string) influxdb2.WriteApiBlocking {
	return c.BaseClient.WriteApiBlocking(org, bucket)
}
