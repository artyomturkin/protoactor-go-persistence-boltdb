package boltdb_test

import (
	"fmt"
	"os"
	"sync"

	"github.com/AsynkronIT/protoactor-go/actor"
	"github.com/AsynkronIT/protoactor-go/persistence"
	"github.com/artyomturkin/protoactor-go-persistence-boltdb"
	"github.com/boltdb/bolt"
)

//DataStore
type exampleDataStore struct {
	providerState persistence.ProviderState
}

func (p *exampleDataStore) GetState() persistence.ProviderState {
	return p.providerState
}

func newExampleDataStore(snapshotInterval int, db *bolt.DB) *exampleDataStore {
	return &exampleDataStore{
		providerState: boltdb.NewBoltProvider(snapshotInterval, db),
	}
}

//Actor
type exampleActor struct {
	persistence.Mixin

	state string
}

//Command to print state with external sync
type Print struct {
	wg *sync.WaitGroup
}

//ensure exampleActor is actor.Actor
var _ actor.Actor = (*exampleActor)(nil)

func (a *exampleActor) Receive(ctx actor.Context) {
	switch msg := ctx.Message().(type) {
	case *persistence.RequestSnapshot:
		// PersistSnapshot when requested
		a.PersistSnapshot(&Snapshot{State: a.state})
	case *Snapshot:
		// Restore from Snapshot
		a.state = msg.State
	case *Event:
		// Persist all events received outside of recovery
		if !a.Recovering() {
			a.PersistReceive(msg)
		}
		// Set state to whatever message says
		a.state = msg.State
	case *Print:
		fmt.Printf("State is %s\n", a.state)
		msg.wg.Done()
	}
}

func exampleActorProducer() actor.Actor {
	return &exampleActor{}
}

func ExampleBoltProvider() {
	tempPath := tempfile()
	defer os.Remove(tempPath)
	db, err := bolt.Open(tempPath, 0666, nil)
	if err != nil {
		panic(err)
	}

	wg := &sync.WaitGroup{}

	props := actor.FromProducer(exampleActorProducer).
		WithMiddleware(
			persistence.Using(newExampleDataStore(4, db)),
		)

	pid, err := actor.SpawnNamed(props, "example.actor")
	if err != nil {
		panic(err)
	}

	//Send some messages to persist
	for index := 0; index < 10; index++ {
		pid.Tell(&Event{State: fmt.Sprintf("event-%d", index)})
	}

	//tell actor to print its state
	wg.Add(1)
	pid.Tell(&Print{wg: wg})
	wg.Wait()

	//stop actor, close db and reopen it
	pid.GracefulPoison()
	db.Close()

	//reopen db and respawn actor
	db, err = bolt.Open(tempPath, 0666, nil)
	if err != nil {
		panic(err)
	}

	props = actor.FromProducer(exampleActorProducer).
		WithMiddleware(
			persistence.Using(newExampleDataStore(4, db)),
		)

	pid, err = actor.SpawnNamed(props, "example.actor")
	if err != nil {
		panic(err)
	}

	//tell actor to print its state
	wg.Add(1)
	pid.Tell(&Print{wg: wg})
	wg.Wait()

	//stop actor, close db and reopen it
	pid.GracefulPoison()
	db.Close()

	// Output:
	// State is event-9
	// State is event-9
}
