package influxdb2robust

import (
	"encoding/json"
)

// batch is a set of lines ready to send to an influx server.
// Each line is a datapoint, encoded using the influxdb line protocol.
//
// When a batch has been stored in BoltDB because it needs to be retried,
// it has an ID.  This is so that when it is successfully uploaded we
// know what to delete from the BoltDB.  Until a batch is put into BoltDB,
// the id pointer is nil.
//
// Batches are serialised using JSON for storage in Bolt.
type batch struct {
	lines []string
	id    *uint64
}

func (b *batch) serialise() ([]byte, error) {
	return json.Marshal(b.lines)
}

func unserialiseBatch(data []byte) (*batch, error) {
	b := new(batch)
	err := json.Unmarshal(data, &b.lines)
	if err != nil {
		return nil, err
	}
	return b, nil
}
