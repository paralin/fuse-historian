package historian

import (
	"github.com/fuserobotics/historian/dbproto"
	"github.com/fuserobotics/reporter/remote"
	"github.com/golang/glog"
	r "gopkg.in/dancannon/gorethink.v2"
	"time"
)

func (h *Historian) Init() error {
	doneChan := make(chan error, 1)
	go h.backgroundSync(doneChan)
	for err := range doneChan {
		return err
	}
	return nil
}

func (h *Historian) backgroundSync(initChan chan error) (disposed bool) {
	initDone := false
	defer func() {
		if initDone || initChan == nil {
			glog.Warningf("Lost connection to streams table changes, retrying...")
			// backoff retry
			time.Sleep(time.Duration(3) * time.Second)
			go h.backgroundSync(nil)
		}
	}()
	cursor, err := h.loadStreams()
	if err != nil {
		glog.Warningf("Error loading streams from db: %v\n", err)
		if initChan != nil {
			initChan <- err
		}
		return
	}

	// Signal we are done initing
	if initChan != nil {
		close(initChan)
	}
	initDone = true

	changesChan := make(chan streamChange)
	cursor.Listen(changesChan)
	for {
		select {
		case <-h.dispose:
			return true
		case cha := <-changesChan:
			h.handleChange(&cha)
		}
		if err := cursor.Err(); err != nil {
			glog.Warningf("Error ")
		}
	}
}

func (h *Historian) handleChange(cha *streamChange) {
	if cha.State != "" {
		return
	}

	invalidHostname := ""

	if cha.OldValue != nil {
		glog.Infof("Removing old stream %s", cha.OldValue.Id)
		delete(h.KnownStreams, cha.OldValue.Id)
		delete(h.Streams, cha.OldValue.Id)
		invalidHostname = cha.OldValue.DeviceHostname
	}

	if cha.NewValue != nil {
		glog.Infof("Adding new stream %s", cha.NewValue.Id)
		h.KnownStreams[cha.NewValue.Id] = cha.NewValue
		invalidHostname = cha.NewValue.DeviceHostname
	}

	if invalidHostname != "" {
		delete(h.RemoteStreamConfigs, invalidHostname)
	}
}

// Full reload: loads in all streams from DB and swaps out maps.
func (h *Historian) loadStreams() (rcrsr *r.Cursor, loadError error) {
	cursor, err := h.StreamsTable.Changes(r.ChangesOpts{
		IncludeInitial: true,
		IncludeStates:  true,
	}).Run(h.rctx)
	defer func() {
		if loadError != nil {
			cursor.Close()
		}
	}()
	if err != nil {
		return nil, err
	}

	h.KnownStreams = make(map[string]*dbproto.Stream)
	h.Streams = make(map[string]*Stream)
	h.RemoteStreamConfigs = make(map[string]*remote.RemoteStreamConfig)

	strm := &streamChange{}
	for cursor.Next(strm) {
		if strm.State == "ready" {
			break
		}
		h.handleChange(strm)
		strm = &streamChange{}
	}

	if err := cursor.Err(); err != nil {
		return nil, err
	}

	return cursor, nil
}
