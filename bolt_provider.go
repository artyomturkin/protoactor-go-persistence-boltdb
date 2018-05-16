package boltdb

import (
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/boltdb/bolt"
	"github.com/golang/protobuf/proto"
)

//BoltProvider persistence provider built on top of boltdb
type BoltProvider struct {
	snapshotInterval int
	mu               sync.RWMutex
	db               *bolt.DB
}

//NewBoltProvider create new persistence provider with boltdb backend
func NewBoltProvider(snapshotInterval int, db *bolt.DB) *BoltProvider {
	return &BoltProvider{
		snapshotInterval: snapshotInterval,
		db:               db,
	}
}

//Restart does who lnows what
func (provider *BoltProvider) Restart() {
	//TODO: find out what it's for
}

//GetSnapshotInterval returns snapshot interval for provider
func (provider *BoltProvider) GetSnapshotInterval() int {
	return provider.snapshotInterval
}

//GetSnapshot returns last snapshot and event index
func (provider *BoltProvider) GetSnapshot(actorName string) (snapshot interface{}, eventIndex int, ok bool) {
	found := true
	err := provider.db.View(func(tx *bolt.Tx) error {
		//Get or create actor bucket
		b := tx.Bucket([]byte(actorName))
		if b == nil {
			found = false
			return nil
		}

		//Get snapshot event index; set found false if `se` does not exist
		eb := b.Get([]byte("se"))
		if eb == nil {
			found = false
			return nil
		}
		eventIndex = decodeInt(eb)

		//Get snapshot; set found false if `sd` does not exist
		sd := b.Get([]byte("sd"))
		if sd == nil {
			snapshot = nil
			return nil
		}

		instance, err := cUnmarshal(sd)
		if err != nil {
			return err
		}
		snapshot = instance

		return nil
	})
	if err != nil {
		panic(fmt.Errorf("failed to read snapshot %d for %s: %v", eventIndex, actorName, err))
	}

	return snapshot, eventIndex, found
}

//PersistSnapshot saves snapshot and event index
func (provider *BoltProvider) PersistSnapshot(actorName string, eventIndex int, snapshot proto.Message) {
	c, err := cMarshal(snapshot)
	if err != nil {
		panic(err)
	}

	err = provider.db.Update(func(tx *bolt.Tx) error {

		b, err := tx.CreateBucketIfNotExists([]byte(actorName))
		if err != nil {
			return fmt.Errorf("failed to create bucket %s: %v", actorName, err)
		}

		err = b.Put([]byte("se"), encodeInt(eventIndex))
		if err != nil {
			return err
		}

		err = b.Put([]byte("sd"), c)
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		panic(err)
	}

}

//GetEvents execute callback for each event in store for actor after event index
func (provider *BoltProvider) GetEvents(actorName string, eventIndexStart int, callback func(e interface{})) {
	events := []proto.Message{}

	err := provider.db.View(func(tx *bolt.Tx) error {
		//Get or create actor bucket
		b := tx.Bucket([]byte(actorName))
		if b == nil {
			return nil
		}

		//Get events bucket
		e := b.Bucket([]byte("events"))
		if e == nil {
			return nil
		}

		//get events after index
		c := e.Cursor()
		for k, v := c.Seek(encodeInt(eventIndexStart)); k != nil; k, v = c.Next() {
			event, err := cUnmarshal(v)
			if err != nil {
				panic(err)
			}
			events = append(events, event)
		}

		return nil
	})
	if err != nil {
		panic(fmt.Errorf("failed to read events for %s: %v", actorName, err))
	}

	for _, event := range events {

		callback(event)
	}
}

//PersistEvent save event into the store
func (provider *BoltProvider) PersistEvent(actorName string, eventIndex int, event proto.Message) {
	c, err := cMarshal(event)
	if err != nil {
		panic(err)
	}

	err = provider.db.Update(func(tx *bolt.Tx) error {

		b, err := tx.CreateBucketIfNotExists([]byte(actorName))
		if err != nil {
			return fmt.Errorf("failed to create bucket %s: %v", actorName, err)
		}

		e, err := b.CreateBucketIfNotExists([]byte("events"))
		if err != nil {
			return fmt.Errorf("failed to create events bucket %s: %v", actorName, err)
		}

		err = e.Put(encodeInt(eventIndex), c)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		panic(err)
	}

}

func encodeInt(i int) []byte {
	v := uint32(i)
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, v)
	return buf
}

func decodeInt(b []byte) int {
	x := binary.BigEndian.Uint32(b)
	return int(x)
}
