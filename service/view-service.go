package service

import (
	"bytes"
	"encoding/json"

	"github.com/fuserobotics/historian"
	"github.com/fuserobotics/historian/dbproto"
	"github.com/fuserobotics/reporter/history"
	"github.com/fuserobotics/reporter/util"
	"github.com/fuserobotics/reporter/view"
	"github.com/fuserobotics/statestream"

	"golang.org/x/net/context"
	r "gopkg.in/dancannon/gorethink.v2"
)

type HistorianViewService struct {
	Historian *historian.Historian
	Session   *r.Session
}

func (h *HistorianViewService) GetState(c context.Context, req *view.GetStateRequest) (*view.GetStateResponse, error) {
	if err := req.Validate(true); err != nil {
		return nil, err
	}

	streamId := historian.StreamTableName(req.Context.HostIdentifier, req.Context.Component, req.Context.StateId)
	strm, err := h.Historian.GetStream(streamId)
	if err != nil {
		return nil, err
	}

	var cursor *stream.Cursor
	if req.Query.Time <= 0 {
		writeCursor, err := strm.StateStream.WriteCursor()
		if err != nil {
			return nil, err
		}
		cursor = writeCursor
	} else {
		cursor = strm.StateStream.BuildCursor(stream.ReadForwardCursor)
		if err := cursor.Init(util.NumberToTime(req.Query.Time)); err != nil {
			return nil, err
		}
	}

	if err := cursor.Error(); err != nil {
		return nil, err
	}

	state, err := cursor.State()
	if err != nil {
		return nil, err
	}

	jsonData, err := json.Marshal(state)
	if err != nil {
		return nil, err
	}

	return &view.GetStateResponse{
		State: &view.StateReport{
			JsonState: string(jsonData),
			Timestamp: util.TimeToNumber(cursor.ComputedTimestamp()),
		},
	}, nil
}

func componentStringId(cmp *dbproto.Stream) string {
	var buf bytes.Buffer
	if cmp.DeviceHostname != "" {
		buf.WriteString(cmp.DeviceHostname)
		buf.WriteString("_")
	}
	buf.WriteString(cmp.ComponentName)
	return buf.String()
}

func (h *HistorianViewService) ListStates(c context.Context, req *view.ListStatesRequest) (*view.ListStatesResponse, error) {
	res := &view.ListStatesResponse{
		List: &view.StateList{
			Components: []*view.StateListComponent{},
		},
	}
	components := make(map[string]*view.StateListComponent)
	for _, stream := range h.Historian.KnownStreams {
		// create a string id for this
		cmpId := componentStringId(stream)
		comp, ok := components[cmpId]
		if !ok {
			comp = &view.StateListComponent{
				HostIdentifier: stream.DeviceHostname,
				Name:           stream.ComponentName,
			}
			components[cmpId] = comp
			res.List.Components = append(res.List.Components, comp)
		}
		comp.States = append(comp.States, &view.StateListState{
			Name:   stream.StateName,
			Config: stream.Config,
		})
	}

	return res, nil
}

func (h *HistorianViewService) GetStateHistory(req *view.StateHistoryRequest, srvstream view.ReporterService_GetStateHistoryServer) error {
	if err := req.Context.Validate(true); err != nil {
		return err
	}
	if err := req.Query.Validate(); err != nil {
		return err
	}

	streame, err := h.Historian.GetStream(
		historian.StreamTableName(
			req.Context.HostIdentifier,
			req.Context.Component,
			req.Context.StateId,
		),
	)
	if err != nil {
		return err
	}

	return history.HandleStateHistoryRequest(req, srvstream, streame.StateStream)
}
