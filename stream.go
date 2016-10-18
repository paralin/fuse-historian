package historian

import (
	"bytes"
	"errors"
	"time"

	"github.com/fuserobotics/historian/dbproto"
	"github.com/fuserobotics/statestream"
	"github.com/golang/glog"
	r "gopkg.in/dancannon/gorethink.v2"
)

var changeUnnecessaryError error = errors.New("change applied locally already")

type Stream struct {
	dispose chan bool
	h       *Historian

	dataTable r.Term

	Data        *dbproto.Stream
	StateStream *stream.Stream
}

// Instantiate a new stream and start watch thread.
func (h *Historian) NewStream(data *dbproto.Stream) (*Stream, error) {
	str := &Stream{
		h:         h,
		dispose:   make(chan bool, 1),
		dataTable: r.Table(DbStreamTableName(data)),
		Data:      data,
	}
	sstr, err := stream.NewStream(str, data.Config)
	if err != nil {
		return nil, err
	}
	str.StateStream = sstr
	/*
		if err := sstr.InitWriter(); err != nil {
			return nil, err
		}
	*/
	go str.watchThread()
	return str, nil
}

// The stream ID is the same as the tabe name.
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

func (s *Stream) watchThread() (watchError error) {
	disposed := false
	select {
	case <-s.dispose:
		disposed = true
		return nil
	default:
	}

	defer func() {
		if watchError != nil {
			glog.Warningf("Error while watching for changes to %s, %v", s.Data.Id, watchError)
		}
		if !disposed && watchError != nil {
			time.Sleep(time.Duration(2) * time.Second)
			go s.watchThread()
		} else {
			glog.Infof("No longer watching for changes to %s.", s.Data.Id)
		}
	}()

	// force a db hit
	s.StateStream.ResetWriter()
	writeCursor, err := s.StateStream.WriteCursor()
	if err != nil {
		return nil
	}

	// Start cursor to watch for new entries
	// this isn't possible since they might come in unordered:
	// query := s.dataTable.Filter(r.Row.Field("timestamp").Gt(writeCursor.ComputedTimestamp()))
	// instead we will just accept we may drop entries in the 1ms between the last read and now.
	cursor, err := s.dataTable.Changes().Run(s.h.rctx)
	if err != nil {
		return err
	}
	defer cursor.Close()

	changesChan := make(chan streamEntryChange)
	cursor.Listen(changesChan)
	glog.Infof("Watching for changes to %s.", s.Data.Id)

	for {
		select {
		case <-s.dispose:
			disposed = true
			return
		case change, ok := <-changesChan:
			if !ok {
				return errors.New("RethinkDB closed the change channel.")
			}
			if err := s.handleChange(&change, writeCursor); err != nil {
				return err
			}
		}
	}
}

func (s *Stream) handleChange(cha *streamEntryChange, writeCursor *stream.Cursor) error {
	// nothing we can do about this
	if cha.NewValue == nil || cha.OldValue != nil {
		return nil
	}

	var wcts time.Time
	// wait until all local writes are done
	writeCursor.WriteGuard(func() error {
		wcts = writeCursor.ComputedTimestamp()
		return nil
	})
	if cha.NewValue.Timestamp.Before(wcts) || cha.NewValue.Timestamp.Equal(wcts) {
		// glog.Infof("Ignoring stream entry change as it's before the latest computed timestamp.")
		return nil
	}
	glog.Infof("Handling stream entry for %s someone else wrote at ts %v local ts %v.", s.Data.Id, cha.NewValue.Timestamp, wcts)
	if err := writeCursor.HandleEntry(cha.NewValue); err != nil {
		return err
	}
	return nil
}

func (s *Stream) dataTableName() string {
	return StreamTableName(s.Data.DeviceHostname, s.Data.ComponentName, s.Data.StateName)
}

func (s *Stream) Dispose() {
	s.dispose <- true
}
