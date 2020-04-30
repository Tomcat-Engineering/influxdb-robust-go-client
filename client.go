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

func NewClientWithOptions(serverUrl string, authToken string, bufferFile string, options *influxdb2.Options) (*InfluxDBRobustClient, error) {
	db, err := NewDatastore(bufferFile)
	if err != nil {
		return nil, err
	}
	return &InfluxDBRobustClient{
		BaseClient: influxdb2.NewClientWithOptions(serverUrl, authToken, options),
		db:         db,
	}, nil
}

func NewClient(serverUrl string, authToken string, bufferFile string) (*InfluxDBRobustClient, error) {
	return NewClientWithOptions(serverUrl, authToken, bufferFile, influxdb2.DefaultOptions())
}

// Delegate most things to the normal client that we are wrapping
func (c *InfluxDBRobustClient) Options() *influxdb2.Options {
	return c.BaseClient.Options()
}

func (c *InfluxDBRobustClient) ServerUrl() string {
	return c.BaseClient.ServerUrl()
}

func (c *InfluxDBRobustClient) Close() {
	for _, w := range c.writeApis {
		w.Close()
	}
	c.BaseClient.Close()
	c.db.Close()
}

func (c *InfluxDBRobustClient) Setup(ctx context.Context, username, password, org, bucket string, retentionPeriodHours int) (*domain.OnboardingResponse, error) {
	return c.BaseClient.Setup(ctx, username, password, org, bucket, retentionPeriodHours)
}

func (c *InfluxDBRobustClient) Ready(ctx context.Context) (bool, error) {
	return c.BaseClient.Ready(ctx)
}

func (c *InfluxDBRobustClient) QueryApi(org string) influxdb2.QueryApi {
	return c.BaseClient.QueryApi(org)
}

func (c *InfluxDBRobustClient) AuthorizationsApi() api.AuthorizationsApi {
	return c.BaseClient.AuthorizationsApi()
}

func (c *InfluxDBRobustClient) OrganizationsApi() api.OrganizationsApi {
	return c.BaseClient.OrganizationsApi()
}

func (c *InfluxDBRobustClient) UsersApi() api.UsersApi {
	return c.BaseClient.UsersApi()
}

// Rather than influxdb2.writeApiImpl, we return our own implementation
func (c *InfluxDBRobustClient) WriteApi(org, bucket string) influxdb2.WriteApi {
	w := NewWriter(c.BaseClient, c.db, org, bucket, c.BaseClient.Options())
	c.writeApis = append(c.writeApis, w)
	return w
}

// Allow direct access to the underlying blocking client - blocking writes will tell the caller
// that the write failed, so we don't need the magic persistent buffer.
func (c *InfluxDBRobustClient) WriteApiBlocking(org, bucket string) influxdb2.WriteApiBlocking {
	return c.BaseClient.WriteApiBlocking(org, bucket)
}
