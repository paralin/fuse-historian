package historian

import (
	"errors"
	"github.com/fuserobotics/historian/dbproto"
	"github.com/fuserobotics/reporter/remote"
	"github.com/fuserobotics/statestream"
	r "gopkg.in/dancannon/gorethink.v2"
)

const streamTableName string = "streams"

// Wrapper for response from RethinkDB with stream change
type streamChange struct {
	NewValue *dbproto.Stream `gorethink:"new_val,omitempty"`
	OldValue *dbproto.Stream `gorethink:"old_val,omitempty"`
	State    string          `gorethink:"state,omitempty"`
}

type streamEntryChange struct {
	NewValue *stream.StreamEntry `gorethink:"new_val,omitempty"`
	OldValue *stream.StreamEntry `gorethink:"old_val,omitempty"`
	State    string              `gorethink:"state,omitempty"`
}

type Historian struct {
	rctx    *r.Session
	dispose chan bool

	StreamsTable r.Term

	// Map of loaded streams
	Streams map[string]*Stream

	// Map of cached remote stream configs
	// Delete to invalidate one
	RemoteStreamConfigs map[string]*remote.RemoteStreamConfig

	// All known streams
	KnownStreams map[string]*dbproto.Stream
}

func NewHistorian(rctx *r.Session) *Historian {
	res := &Historian{
		rctx:                rctx,
		dispose:             make(chan bool, 1),
		Streams:             make(map[string]*Stream),
		RemoteStreamConfigs: make(map[string]*remote.RemoteStreamConfig),
		KnownStreams:        make(map[string]*dbproto.Stream),
		StreamsTable:        r.Table(streamTableName),
	}
	return res
}

// Returns pre-loaded stream or gets from DB
func (h *Historian) GetStream(id string) (str *Stream, ferr error) {
	if str, ok := h.Streams[id]; ok {
		return str, nil
	}

	data, ok := h.KnownStreams[id]
	if !ok {
		return nil, errors.New("Stream not known.")
	}

	str, err := h.NewStream(data)
	if err != nil {
		return nil, err
	}

	// Note: be sure to call Dispose() when deleting.
	h.Streams[id] = str
	return str, nil
}

func (h *Historian) GetDeviceStreams(hostname string) ([]*dbproto.Stream, error) {
	res := []*dbproto.Stream{}

	for _, stream := range h.KnownStreams {
		if stream.DeviceHostname != hostname {
			continue
		}
		res = append(res, stream)
	}

	return res, nil
}

func (h *Historian) BuildRemoteStreamConfig(hostname string) (*remote.RemoteStreamConfig, error) {
	if resa, ok := h.RemoteStreamConfigs[hostname]; ok {
		return resa, nil
	}

	streams, err := h.GetDeviceStreams(hostname)
	if err != nil {
		return nil, err
	}
	res := &remote.RemoteStreamConfig{}
	for _, stream := range streams {
		rstream := &remote.RemoteStreamConfig_Stream{
			ComponentId: stream.ComponentName,
			StateId:     stream.StateName,
		}
		res.Streams = append(res.Streams, rstream)
	}
	res.FillCrc32()
	h.RemoteStreamConfigs[hostname] = res
	return res, nil
}
