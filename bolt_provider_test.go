package boltdb_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	"testing"

	"github.com/AsynkronIT/protoactor-go/actor"
	"github.com/AsynkronIT/protoactor-go/persistence"
	boltdb "github.com/artyomturkin/protoactor-go-persistence-boltdb"
	"github.com/boltdb/bolt"
)

const ActorName = "test.actor"

type dataStore struct {
	providerState persistence.ProviderState
	db            *bolt.DB
	tpath         string
}

func (p *dataStore) GetState() persistence.ProviderState {
	return p.providerState
}

func (d *dataStore) Close() error {
	defer os.Remove(d.tpath)
	return d.db.Close()
}

// initData sets up a data store
// it adds one event to set state for every sting passed in
// set the last snapshot to given index of those events
func initData(t *testing.T, c int, snapshotInterval, lastSnapshot int, states ...string) *dataStore {
	tpath := tempfile()
	db, err := bolt.Open(tpath, 0666, nil)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	// add all events
	state := boltdb.NewBoltProvider(snapshotInterval, db)
	for i, s := range states {
		state.PersistEvent(ActorName, i, &Event{State: s})
		t.Logf("case %d - init event %v", c, &Event{State: s})
	}
	// mark one as a snapshot
	if lastSnapshot < len(states) {
		snapshot := states[lastSnapshot]
		state.PersistSnapshot(
			ActorName, lastSnapshot, &Snapshot{State: snapshot},
		)
		t.Logf("case %d - init snap %d %v", c, lastSnapshot, &Snapshot{State: snapshot})
	}
	return &dataStore{providerState: state, db: db, tpath: tpath}
}

type myActor struct {
	persistence.Mixin
	state string
	t     *testing.T
	mod   string
}

var _ actor.Actor = (*myActor)(nil)

func makeActor(t *testing.T) func() actor.Actor {
	return func() actor.Actor {
		return &myActor{t: t}
	}
}

var queryWg sync.WaitGroup
var mod string
var queryState map[string]string = map[string]string{}

func (a *myActor) Receive(ctx actor.Context) {
	switch msg := ctx.Message().(type) {
	case *persistence.RequestSnapshot:
		// PersistSnapshot when requested
		a.PersistSnapshot(&Snapshot{State: a.state})
		a.t.Logf("%s-%s pers %v", ctx.Self().Id, mod, &Snapshot{State: a.state})
	case *Snapshot:
		// Restore from Snapshot
		a.state = msg.State
		a.t.Logf("%s-%s snap %v", ctx.Self().Id, mod, msg)
	case *Event:
		// Persist all events received outside of recovery
		if !a.Recovering() {
			a.PersistReceive(msg)
		}
		// Set state to whatever message says
		a.state = msg.State
		a.t.Logf("%s-%s even %v", ctx.Self().Id, mod, msg)
	case *Query:
		queryState[ctx.Self().Id] = a.state
		queryWg.Done()
	}
}

type Query struct{}

func TestSnapshotting(t *testing.T) {
	cases := []struct {
		init      *dataStore
		msgs      []string
		afterMsgs string
	}{
		// replay with no state
		0: {initData(t, 0, 5, 0), nil, ""},

		// replay directly on snapshot, no more messages
		1: {initData(t, 1, 8, 2, "a", "b", "c"), nil, "c"},

		// replay with snapshot and events, add another event
		2: {initData(t, 2, 8, 1, "a", "b", "c"), []string{"d"}, "d"},

		// replay state and add an event, which triggers snapshot
		3: {initData(t, 3, 4, 1, "a", "b", "c"), []string{"d"}, "d"},

		// replay state and add an event, which triggers snapshot,
		// and then another one
		4: {initData(t, 4, 4, 1, "a", "b", "c"), []string{"d", "e"}, "e"},
	}

	for i, tc := range cases {
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			actorName := fmt.Sprintf("%s.%d", ActorName, i)

			props := actor.FromProducer(makeActor(t)).
				WithMiddleware(persistence.Using(tc.init))

			mod = "orig"
			pid, err := actor.SpawnNamed(props, actorName)
			noError(t, "spawn actor", err)

			// send a bunch of messages
			for _, msg := range tc.msgs {
				pid.Tell(&Event{State: msg})
			}

			queryWg.Add(1)
			pid.Tell(&Query{})
			queryWg.Wait()
			// check the state after all these messages
			equal(t, "spawned actor last message", tc.afterMsgs, queryState[actorName])

			// wait for shutdown
			pid.GracefulPoison()

			mod = "resp"
			pid, err = actor.SpawnNamed(props, actorName)
			noError(t, "respawn actor", err)

			queryWg.Add(1)
			pid.Tell(&Query{})
			queryWg.Wait()
			// check the state after all these messages
			equal(t, "respawned actor last message", tc.afterMsgs, queryState[actorName])

			// shutdown at end of test for cleanup
			pid.GracefulPoison()
			tc.init.Close()
		})
	}
}

func equal(t *testing.T, name string, expected, actual interface{}) {
	if expected != actual {
		t.Errorf("%s expected: %v, instead got: %v", name, expected, actual)
	}
}

func noError(t *testing.T, name string, err error) {
	if err != nil {
		t.Fatalf("%s failed with error: %v", name, err)
	}
}

func tempfile() string {
	f, err := ioutil.TempFile("", "bolt-")
	if err != nil {
		panic(err)
	}
	if err := f.Close(); err != nil {
		panic(err)
	}
	if err := os.Remove(f.Name()); err != nil {
		panic(err)
	}
	return f.Name()
}
