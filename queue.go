package influxdb2robust

import (
	"encoding/binary"
	"fmt"
	"log"
	"time"

	bolt "go.etcd.io/bbolt"
)

// retryQueue is a BoltDB database which stores `batch`es of influxdb lines.
// Except for initialisation, errors are logged rather than returned.
type retryQueue struct {
	db *bolt.DB
}

func newRetryQueue(filename string) (*retryQueue, error) {
	db, err := bolt.Open(filename, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("Failed to open database: %s", err)
	}

	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("q"))
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("Failed to create bucket: %s", err)
	}

	return &retryQueue{
		db: db,
	}, nil
}

func (q *retryQueue) close() {
	q.db.Close()
}

func (q *retryQueue) first() *batch {
	var b *batch
	q.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("q"))
		cursor := bucket.Cursor()
		id, data := cursor.First()
		if id == nil {
			// There are no batches
			return nil
		}

		var err error
		b, err = unserialiseBatch(data)
		if err != nil {
			log.Printf("Failed to decode data from disk: %s", err)
			return nil
		}

		intId := btoi(id)
		b.id = &intId
		return nil
	})
	return b
}

func (q *retryQueue) store(b *batch) {
	if b == nil {
		return
	}

	data, err := b.serialise()
	if err != nil {
		log.Printf("Failed to encode data to store onto disk: %s", err)
		return
	}

	var id uint64
	err = q.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("q"))
		id, _ = bucket.NextSequence() // Apparently this can't return an error inside an Update()
		err = bucket.Put(itob(id), data)
		return err
	})
	if err == nil {
		b.id = &id
	} else {
		log.Printf("DB error in store: %s", err)
	}
}

func (q *retryQueue) markDone(id uint64) {
	err := q.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("q"))
		if bucket == nil {
			// The bucket doesn't exist, oh well
			return nil
		}
		return bucket.Delete(itob(id))
	})
	if err != nil {
		log.Printf("DB error in markDone: %s", err)
	}
}

// itob returns an 8-byte big endian representation of v.
func itob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

// btoi converts 8 bytes of binary data into a uint64
func btoi(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}
