package main

import (
	"fmt"
	"log"

	bolt "go.etcd.io/bbolt"
)

type PtWithMeta struct {
	Org string
	Bucket string
	Line string
	Id uint64
}

type client struct {
	Org string
	Bucket string
	NewDataChan chan *PtWithMeta
	FirstLivePoint *PtWithMeta // So that backfill knows when to stop.
}

type struct datastore {
	db *bolt.DB
	In chan   *PtWithMeta
	Done chan *PtWithMeta
	clients []*client
}

func NewDatastore(filename string) (*datastore, error) {
	db, err := bolt.Open(filename, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("Failed to open database: %s", err)
	}

	d := datastore{
		db: db,
		In: make(chan *PtWithMeta),
		Done: make(chan *PtWithMeta),
	}
	go d.run()
	return &d, nil
}

// Returns a channel which will squirt out datapoints which need uploading
// for this org/bucket (including old ones).
func (d *datastore) GetNewDataChannel(org, bucket) (chan *PtWithMeta) {
	c := client{
		Org: org,
		Bucket: bucket,
		NewDataChan: make(chan *PtWithMeta),
	}
	go d.backfill(c)
	d.clients = append(d.clients, c)
	return c.NewDataChan
}

// Run forever, moving data between the channels and the Bolt database.
// Only stuff called from this function will update the database.
func (d* datastore) run() {
	for {
		select {
		case pt <- d.In:
			d.store(pt) // This sets the point's ID field
			for _, c := range(d.clients) {
				if c.Org == pt.Org && c.Bucket == pt.Bucket {
					c.NewDataChan <- pt
					if c.FirstLivePoint == nil {
						c.FirstLivePoint = pt
					}
				}
			}
		case pt <- d.Done:
			d.markDone(pt)
		}
	}
}

func (d* datastore) store(pt *PtWithMeta) {
	id uint64
	err := d.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(topic(pt.Org, pt.Bucket))
		if err != nil {
			return fmt.Errorf("Failed to create topic bucket: %s", err)
		}
		id, _ = b.NextSequence() // Apparently this can't return an error inside an Update()
		err = b.Put(itob(id), []byte(pt.Line))
		return err
	})			
	if err == nil {
		pt.Id = id
	} else {
		log.Printf("DB error in store: %s", err)
	}
}

func (d* datastore) markDone(pt *PtWithMeta) {
	err := d.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(topic(pt.Org, pt.Bucket))
		if b == nil {
			// The bucket doesn't exist, oh well
			return nil
		}
		return b.Delete(itob(Pt.Id))
	})
	if err != nil {
		log.Printf("DB error in markDone: %s", err)
	}
}

// Read data from the database and stick it into the client's channel.
func (d *datastore) backfill(c *client) {
	err := d.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(topic(c.Org, c.Bucket))
		if b == nil {
			// The bucket doesn't exist, therefore there is no data to backfill
			return nil
		}
		
		// Iterate over all the points on this topic, copying them into the client's "new data" channel
		cursor := b.Cursor()
		for id, line := cursor.First(); id != nil; id, line = cursor.Next() {
			if c.FirstLivePoint != nil && id >= c.FirstLivePoint.Id {
				// We have caught up with the live data
				break
			}
			c.NewDataChan <- &PtWithMeta{
				Org: c.Org,
				Bucket: c.Bucket,
				Line: line,
				Id: btoi(id),
			}
		}
		return nil
	})
	if err != nil {
		log.Printf("DB error in backfill: %s", err)
	}
}

// Topic name (used inside bolt only)
func topic(org, bucket string) []byte{
	// Sorry, a bit hacky
	return []byte(org + "///" + bucket)
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
