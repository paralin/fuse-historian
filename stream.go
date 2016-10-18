package historian

import (
	"bytes"
	"github.com/fuserobotics/historian/dbproto"
	"github.com/fuserobotics/statestream"
	r "gopkg.in/dancannon/gorethink.v2"
)

type Stream struct {
	h *Historian

	dataTable r.Term

	Data        *dbproto.Stream
	StateStream *stream.Stream
}

// Instantiate a new stream
func (h *Historian) NewStream(data *dbproto.Stream) (*Stream, error) {
	str := &Stream{
		h:         h,
		dataTable: r.Table(DbStreamTableName(data)),
		Data:      data,
	}
	sstr, err := stream.NewStream(str, data.Config)
	if err != nil {
		return nil, err
	}
	str.StateStream = sstr
	if err := sstr.InitWriter(); err != nil {
		return nil, err
	}
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

func (s *Stream) dataTableName() string {
	return StreamTableName(s.Data.DeviceHostname, s.Data.ComponentName, s.Data.StateName)
}
