package historian

import (
	"bytes"
	"errors"
	"github.com/fuserobotics/historian/dbproto"
	"github.com/fuserobotics/statestream"
	r "gopkg.in/dancannon/gorethink.v2"
)

type Stream struct {
	h           *Historian
	loadError   chan error
	loadSuccess chan bool

	dispose     chan bool
	disposed    bool
	disposeFunc []func()

	dataTable r.Term

	Data        dbproto.Stream
	StateStream *stream.Stream
}

func (s *Stream) Init(id string) error {
	go s.loadAndWatch(id)
	select {
	case err := <-s.loadError:
		return err
	case <-s.loadSuccess:
		return nil
	}
}

func (s *Stream) OnDispose(cb func()) {
	s.disposeFunc = append(s.disposeFunc, cb)
}

// Wrapper for response from RethinkDB with stream change
type streamChange struct {
	NewValue *dbproto.Stream `gorethink:"new_val,omitempty"`
	OldValue *dbproto.Stream `gorethink:"old_val,omitempty"`
	State    string          `gorethink:"state,omitempty"`
}

func StreamTableName(deviceHostname, componentName, stateName string) string {
	var buf bytes.Buffer

	if deviceHostname != "" {
		buf.WriteString(deviceHostname)
		buf.WriteString("_")
	}

	if componentName != "" {
		buf.WriteString(componentName)
		buf.WriteString("_")
	}

	buf.WriteString(stateName)

	return buf.String()
}

func DbStreamTableName(stream *dbproto.Stream) string {
	return StreamTableName(stream.DeviceHostname, stream.ComponentName, stream.StateName)
}

func (s *Stream) dataTableName() string {
	return StreamTableName(s.Data.DeviceHostname, s.Data.ComponentName, s.Data.StateName)
}

// Load the stream from the DB and watch for changes.
func (s *Stream) loadAndWatch(id string) (loadError error) {
	loaded := false
	defer func() {
		if !loaded && loadError != nil {
			s.loadError <- loadError
		}
	}()

	// Perform initial load from DB.
	cursor, err := s.h.StreamsTable.Get(id).Changes(r.ChangesOpts{
		IncludeInitial: true,
	}).Run(s.h.rctx)
	if err != nil {
		return err
	}
	defer cursor.Close()
	cr := streamChange{}
	if err := cursor.One(&cr); err != nil {
		return err
	}
	if cr.NewValue == nil {
		return errors.New("Stream not found.")
	}
	s.Data = *cr.NewValue

	// Set the table name
	s.dataTable = r.Table(s.dataTableName())

	// Init the state stream
	{
		strm, err := stream.NewStream(s, s.Data.Config)
		if err != nil {
			return err
		}
		if _, err := strm.WriteCursor(); err != nil {
			return err
		}
		s.StateStream = strm
	}

	s.loadSuccess <- true
	loaded = true

	// Watch for changes
	ch := make(chan streamChange)
	cursor.Listen(ch)
ChangesLoop:
	for !s.disposed {
		select {
		case <-s.dispose:
			break ChangesLoop
		case cr = <-ch:
		}
		if cr.NewValue == nil {
			s.Dispose()
			break ChangesLoop
		} else {
			s.Data = *cr.NewValue
		}
	}

	return nil
}

func (s *Stream) Dispose() {
	if s.disposed {
		return
	}

	s.dispose <- true
	s.disposed = true
	for _, cb := range s.disposeFunc {
		cb()
	}
}
