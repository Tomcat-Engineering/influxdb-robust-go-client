package influxdb2robust

import (
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"
)

type retryQueue struct {
	db *bolt.DB
}

func newRetryQueue(filename string) (*retryQueue, error) {
	db, err := bolt.Open(filename, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("Failed to open database: %s", err)
	}
	return &retryQueue{
		db: db,
	}, nil
}

func (q *retryQueue) close() {
	q.db.Close()
}

func (w *retryQueue) first() *batch {
	return nil
}

func (q *retryQueue) store(*batch) {

}

func (q *retryQueue) markDone(id uint64) {

}
