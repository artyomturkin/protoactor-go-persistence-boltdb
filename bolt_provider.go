package boltdb

import (
	"fmt"
	"reflect"
	"strconv"
	"sync"

	"github.com/boltdb/bolt"
	gogoproto "github.com/gogo/protobuf/proto"
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
		ei, err := strconv.Atoi(string(eb))
		if err != nil {
			return fmt.Errorf("%v", err)
		}
		eventIndex = ei

		//Get snapshot event type; set found false if `st` does not exist
		tb := b.Get([]byte("st"))
		if tb == nil {
			found = false
			return nil
		}
		ts := string(tb)
		protoType := gogoproto.MessageType(ts)
		if protoType == nil {
			return fmt.Errorf("unknown message type %s for snapshot %s: %v", ts, actorName, protoType)
		}
		t := protoType.Elem()
		intPtr := reflect.New(t)
		instance := intPtr.Interface().(gogoproto.Message)

		//Get snapshot data;
		sb := b.Get([]byte("sd"))
		if sb == nil {
			snapshot = nil
			return nil
		}

		proto.Unmarshal(sb, instance)
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
	typeName := gogoproto.MessageName(snapshot)
	bs, err := gogoproto.Marshal(snapshot)
	eis := strconv.Itoa(eventIndex)

	if err != nil {
		panic(fmt.Errorf("failed to marshal snapshot:%v", err))
	}

	err = provider.db.Update(func(tx *bolt.Tx) error {

		b, err := tx.CreateBucketIfNotExists([]byte(actorName))
		if err != nil {
			return fmt.Errorf("failed to create bucket %s: %v", actorName, err)
		}

		err = b.Put([]byte("se"), []byte(eis))
		if err != nil {
			return err
		}
		err = b.Put([]byte("st"), []byte(typeName))
		if err != nil {
			return err
		}
		err = b.Put([]byte("sd"), bs)
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		panic(fmt.Errorf("failed to persist snapshot %d for %s: %v", eventIndex, actorName, err))
	}
}

//GetEvents execute callback for each event in store for actor after event index
func (provider *BoltProvider) GetEvents(actorName string, eventIndexStart int, callback func(e interface{})) {

}

//PersistEvent save event into the store
func (provider *BoltProvider) PersistEvent(actorName string, eventIndex int, event proto.Message) {

}

type Marshalable interface {
	Marshal() ([]byte, error)
}
