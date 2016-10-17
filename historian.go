package historian

import (
	"github.com/fuserobotics/historian/dbproto"
	"github.com/fuserobotics/reporter/remote"
	r "gopkg.in/dancannon/gorethink.v2"
)

const streamTableName string = "streams"

type Historian struct {
	rctx *r.Session

	StreamsTable r.Term

	// Map of loaded streams
	Streams map[string]*Stream
}

func NewHistorian(rctx *r.Session) *Historian {
	res := &Historian{rctx: rctx, Streams: make(map[string]*Stream)}
	res.StreamsTable = r.Table(streamTableName)
	return res
}

// Returns pre-loaded stream or gets from DB
func (h *Historian) GetStream(id string) (str *Stream, ferr error) {
	str, ok := h.Streams[id]
	if ok {
		return str, nil
	}

	str = &Stream{
		h:           h,
		dispose:     make(chan bool, 1),
		loadError:   make(chan error, 1),
		loadSuccess: make(chan bool, 1),
	}
	if err := str.Init(id); err != nil {
		return nil, err
	}

	h.Streams[id] = str
	str.OnDispose(func() {
		delete(h.Streams, id)
	})
	return str, nil
}

func (h *Historian) GetDeviceStreams(hostname string) ([]*Stream, error) {
	res := []*Stream{}
	cursor, err := h.StreamsTable.Filter(r.Row.Field("device_hostname").Eq(hostname)).Run(h.rctx)
	defer cursor.Close()
	if err != nil {
		return res, err
	}

	dbstream := &dbproto.Stream{}
	for cursor.Next(dbstream) {
		tableName := DbStreamTableName(dbstream)
		nr, err := h.GetStream(tableName)
		if err != nil {
			return res, err
		}

		res = append(res, nr)
		dbstream = &dbproto.Stream{}
	}
	if err := cursor.Err(); err != nil {
		return res, err
	}
	return res, nil
}

func (h *Historian) BuildRemoteStreamConfig(hostname string) (*remote.RemoteStreamConfig, error) {
	streams, err := h.GetDeviceStreams(hostname)
	if err != nil {
		return nil, err
	}
	res := &remote.RemoteStreamConfig{}
	for _, stream := range streams {
		rstream := &remote.RemoteStreamConfig_Stream{
			ComponentId: stream.Data.ComponentName,
			StateId:     stream.Data.StateName,
		}
		res.Streams = append(res.Streams, rstream)
	}
	res.FillCrc32()
	return res, nil
}
