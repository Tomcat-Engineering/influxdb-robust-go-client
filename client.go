// package "influxdb2robust"
package main

import (
	"context"

	"github.com/influxdata/influxdb-client-go"
)


type InfluxDBRobustClient struct {
	BaseClient influxdb2.InfluxDBClient
	db *datastore
}


func NewClientWithOptions(serverUrl string, authToken string, bufferFile string, options *influxdb2.Options) (*InfluxDBRobustClient, error) {
	db, err := NewDatastore(bufferFile)
	if err != nil {
		return nil, err
	}
	return &InfluxDBRobustClient{
		BaseClient: influxdb2.NewClientWithOptions(serverUrl, authToken, options),
		db: db,
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
	c.BaseClient.Close()
}

func (c *InfluxDBRobustClient) Ready(ctx context.Context) (bool, error) {
	return c.BaseClient.Ready(ctx)
}

func (c *InfluxDBRobustClient) QueryApi(org string) influxdb2.QueryApi {
	return c.BaseClient.QueryApi(org)
}

// Rather than influxdb2.writeApiImpl, we return our own implementation
func (c *InfluxDBRobustClient) WriteApi(org, bucket string) influxdb2.WriteApi {
	return NewWriter(c.BaseClient, c.db, org, bucket, c.BaseClient.Options())
}