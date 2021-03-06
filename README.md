# Proto Actor [Go] - BoltDB persistence provider

[![Go Report Card](https://goreportcard.com/badge/github.com/artyomturkin/protoactor-go-persistence-boltdb)](https://goreportcard.com/report/github.com/artyomturkin/protoactor-go-persistence-boltdb)

Go package with persistence provider for Proto Actor (Go) based on BoltDB.

## Get started

Install package

```
go get github.com/artyomturkin/protoactor-go-persistence-boltdb
```

Create event and snapshot proto messages. Messages can be named differently.
```protobuf
syntax = "proto3";
package main;

message Event {
    string state = 1;
}

message Snapshot {
    string state = 1;
}
```

Generate go types from proto file
```
protoc --gogoslick_out=. test.proto
```

Create DataStore implementation
```go
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
```

Create Actor with persistence implementation
```go
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
```

Create actor.Producer implementation
```go
func exampleActorProducer() actor.Actor {
	return &exampleActor{}
}
```

In open bolt database and pass it to actor.Props
```go
func main() {
	db, err := bolt.Open(tempPath, 0666, nil)
	if err != nil {
		panic(err)
	}

	props := actor.FromProducer(exampleActorProducer).
		WithMiddleware(
			persistence.Using(newExampleDataStore(4, db)),
		)
	............
}
```

Spawn actor and send it some events to set state, ask actor to print its state
```go
func main() {
	............
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
	............
}
```

Stop actor and db, then restart everything
```go
func main() {
	............
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
}
```

Running example will output:
```
State is event-9
State is event-9
```