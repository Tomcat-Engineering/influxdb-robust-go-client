// package "influxdb2robust"
package main

import (
	"github.com/influxdata/influxdb-client-go"
)


type InfluxDBRobustClient struct {
	BaseClient influxdb2.InfluxDBClient
	db *datastore
}


func NewClientWithOptions(serverUrl string, authToken string, bufferFile string, options *influxdb2.Options) {
	return &InfluxDBRobustClient{
		BaseClient: influxdb2.NewClientWithOptions(serverUrl, authToken, options),
		db: NewDatastore(bufferFile),
	}
}

func NewClient(serverUrl string, authToken string, bufferFile string) *InfluxDBRobustClient {
	return NewClientWithOptions(serverUrl, authToken, bufferFile, influxdb2.DefaultOptions())
}

// Delegate most things to the normal client that we are wrapping
func (c *InfluxDBRobustClient) Options() *Options {
	return c.BaseClient.Options()
}

func (c *InfluxDBRobustClient) ServerUrl() string {
	return c.BaseClient.ServerUrl()
}

func (c *InfluxDBRobustClient) Close() {
	return c.BaseClient.Close()
}

func (c *InfluxDBRobustClient) Ready(ctx context.Context) (bool, error) {
	return c.BaseClient.Ready(ctx)
}

func (c *InfluxDBRobustClient) QueryApi(org string) influxdb2.QueryApi {
	return c.BaseClient.QueryApi(org)
}

// Rather than influxdb2.writeApiImpl, we return our own implementation
func (c *InfluxDBRobustClient) WriteApi(org, bucket string) influxdb2.WriteApi {
	return NewWriter(&c.BaseClient, org, bucket)
}