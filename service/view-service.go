package service

import (
	"bytes"
	"encoding/json"
	"errors"
	"time"

	"github.com/fuserobotics/historian"
	"github.com/fuserobotics/historian/dbproto"
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

	strm := streame.StateStream
	var cursor *stream.Cursor

	// first send the history
	earlyBound := util.NumberToTime(req.Query.BeginTime)
	lateBound := util.NumberToTime(req.Query.EndTime)
	if req.Query.EndTime == 0 {
		lateBound = time.Now()
	}

	cursor = strm.BuildCursor(stream.ReadForwardCursor)
	ch := make(chan *stream.StreamEntry)
	sub := cursor.SubscribeEntries(ch)
	// spawn a goroutine to send the data, while simultaneously fast forwarding
	go func() {
		for {
			select {
			case <-srvstream.Context().Done():
				return
			case entry, ok := <-ch:
				if !ok {
					return
				}
				jsonData, err := json.Marshal(entry.Data)
				if err != nil {
					break
				}
				// not sure what to do if we get an error here.
				srvstream.Send(&view.StateHistoryResponse{
					Status: view.StateHistoryResponse_HISTORY_INITIAL_SET,
					State: &view.StateEntry{
						JsonState: string(jsonData),
						Timestamp: util.TimeToNumber(entry.Timestamp),
						Type:      int32(entry.Type),
					},
				})
			}
		}
	}()

	if err := cursor.Init(earlyBound); err != nil {
		if err == stream.NoDataError {
			// Check if we can get something after it.
			entry, err := streame.GetEntryAfter(earlyBound, stream.StreamEntrySnapshot)
			if err != nil {
				return err
			}
			if entry == nil || entry.Timestamp.After(lateBound) {
				return stream.NoDataError
			}
			earlyBound = entry.Timestamp
			if err := cursor.InitWithSnapshot(entry); err != nil {
				return err
			}
			// Send the initial snapshot entry
			ch <- entry
		} else {
			return err
		}
	}

	cursor.SetTimestamp(lateBound)
	if err := cursor.ComputeState(); err != nil {
		return err
	}

	// close the history channel
	sub.Unsubscribe()
	close(ch)

	// if we need to tail, grab the write cursor.
	if !req.Query.Tail {
		return nil
	}

	cursor, err = streame.StateStream.WriteCursor()
	if err != nil {
		return err
	}
	if !cursor.Ready() {
		return errors.New("Try again.")
	}

	// Signal to the client the initial set is done
	if err := srvstream.Send(&view.StateHistoryResponse{Status: view.StateHistoryResponse_HISTORY_TAIL}); err != nil {
		return err
	}

	// Buffer this one, to make sure we don't slow down writing.
	wch := make(chan *stream.StreamEntry, 100)
	sub = cursor.SubscribeEntries(wch)
	defer func() {
		sub.Unsubscribe()
	}()

	for {
		select {
		case ent := <-wch:
			jsonData, err := json.Marshal(ent.Data)
			if err != nil {
				return err
			}
			srvstream.Send(&view.StateHistoryResponse{
				Status: view.StateHistoryResponse_HISTORY_TAIL,
				State: &view.StateEntry{
					JsonState: string(jsonData),
					Timestamp: util.TimeToNumber(ent.Timestamp),
					Type:      int32(ent.Type),
				},
			})
		case <-srvstream.Context().Done():
			return nil
		}
	}
}
