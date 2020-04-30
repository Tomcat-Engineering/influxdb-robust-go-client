// package influxdb2robust provides a variant of the standard influxdb2 go client
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

type InfluxDBRobustClient struct {
	BaseClient influxdb2.InfluxDBClient
	db         *datastore
	writeApis  []influxdb2.WriteApi
}

// NewClientWithOptions creates Client for connecting to given serverUrl with provided authentication token
// and configured with custom Options
// Authentication token can be empty in case of connecting to newly installed InfluxDB server, which has not been set up yet.
// In such case Setup will set authentication token
func NewClientWithOptions(serverUrl string, authToken string, bufferFile string, options *influxdb2.Options) (*InfluxDBRobustClient, error) {
	db, err := newDatastore(bufferFile)
	if err != nil {
		return nil, err
	}
	return &InfluxDBRobustClient{
		BaseClient: influxdb2.NewClientWithOptions(serverUrl, authToken, options),
		db:         db,
	}, nil
}

// NewClient creates Client for connecting to given serverUrl with provided authentication token, with the default options.
// Authentication token can be empty in case of connecting to newly installed InfluxDB server, which has not been set up yet.
// In such case Setup will set authentication token
func NewClient(serverUrl string, authToken string, bufferFile string) (*InfluxDBRobustClient, error) {
	return NewClientWithOptions(serverUrl, authToken, bufferFile, influxdb2.DefaultOptions())
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
	c.db.Close()
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
func (c *InfluxDBRobustClient) WriteApi(org, bucket string) influxdb2.WriteApi {
	w := newWriter(c.BaseClient, c.db, org, bucket, c.BaseClient.Options())
	c.writeApis = append(c.writeApis, w)
	return w
}

// WriteApi returns the synchronous, blocking, Write client.
// We allow direct access to the underlying blocking client - blocking writes will tell the caller
// that the write failed, so we don't need the magic persistent buffer.
func (c *InfluxDBRobustClient) WriteApiBlocking(org, bucket string) influxdb2.WriteApiBlocking {
	return c.BaseClient.WriteApiBlocking(org, bucket)
}
