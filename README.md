# Robust InfluxDB 2 Golang Client

[![Go Report Card](https://goreportcard.com/badge/github.com/Tomcat-Engineering/influxdb-robust-go-client)](https://goreportcard.com/report/github.com/Tomcat-Engineering/influxdb-robust-go-client)

*Golang client for Influxdb version 2 which uses a local BoltDB buffer to tolerate network outages when uploading data.*

The [official InfluxDB 2 golang client](https://github.com/influxdata/influxdb-client-go) works great when your code can reliably communicate with the InfluxDB instance.  However, if you are trying to write datapoints into a remote InfluxDB instance over an unreliable connection then it is not so good - if the network is down, by default it will try to write each batch of points three times at one second intervals, and then give up and throw your data away.

This "robust" version is a thin wrapper around the official InfluxDB 2 golang client which just modifies the `WriteApi()` method to return a special WriteApi.  When it can't reach InfluxDB it stores data into a local BoltDB database instead, and tries again later.  Once the data has been safely uploaded into InfluxDB it is deleted from the local database.  

This means that we can tolerate network outages of several days (not uncommon in the industrial environments that this was designed for) - in fact you can buffer as much data as will fit on your hard drive.  When the network starts working again, the system will automatically back-fill all the data (maintaining write order so that InfluxDB can ingest it efficiently).  

Buffering the data on disk rather than in memory means that you can safely restart your software at any time without losing your buffer full of data.

## How to Use It

Import this package as well the standard `influxdb2` one.  Instantiate the client using this package, create a write API, then feed it points which you create using the standard `influxdb2` library.

```
package main

import (
    "fmt"
    "time"

    "github.com/influxdata/influxdb-client-go"
    "github.com/tomcat-engineering/influxdb-robust-go-client"
)

func main() {
    // Create new client with default options
    client := influxdb2robust.NewClient("http://localhost:9999", "my-token")

    defer client.Close()

    // Create a Write API instance for a particular organisation and data bucket
    // Note that compared to the standard influxdb2 version,
    // this needs an extra filename arg for the buffer database,
    // and returns an extra error field
    writeApi, err := client.WriteApi("my-org", "my-bucket", "my-bufferfile.db")
    if err != nil {
        fmt.Printf("Error initialising write API: %s", err)
        return
    }

    // Create point using fluent style - note that this is using the standard 
    // influxdb2 package, not influxdb2robust 
    p := influxdb2.NewPointWithMeasurement("stat").
        AddTag("unit", "temperature").
        AddField("avg", 23.2).
        AddField("max", 45).
        SetTime(time.Now())
    
    // Upload the point to the server.  If the server is not available then
    // it will be stored in the buffer file and uploaded later.  
    writeApi.WritePoint(p)
    
    // You can also store points using the InfluxDB line protocol
    line := fmt.Sprintf("stat,unit=temperature avg=%f,max=%f", 23.5, 45.0)
    writeApi.WriteRecord(line)
   
}
```
