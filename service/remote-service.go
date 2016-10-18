package service

import (
	"encoding/json"
	"errors"

	"github.com/fuserobotics/historian"
	"github.com/fuserobotics/reporter/remote"
	"github.com/fuserobotics/reporter/util"
	"github.com/fuserobotics/statestream"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	r "gopkg.in/dancannon/gorethink.v2"
)

type HistorianRemoteService struct {
	Historian *historian.Historian
	Session   *r.Session
}

func (s *HistorianRemoteService) GetRemoteConfig(c context.Context, req *remote.GetRemoteConfigRequest) (*remote.GetRemoteConfigResponse, error) {
	if err := req.Context.Validate(); err != nil {
		return nil, err
	}

	hostId := req.Context.HostIdentifier
	cfg, err := s.Historian.BuildRemoteStreamConfig(hostId)
	if err != nil {
		return nil, err
	}

	return &remote.GetRemoteConfigResponse{Config: cfg}, nil
}

func (s *HistorianRemoteService) PushStreamEntry(c context.Context, req *remote.PushStreamEntryRequest) (*remote.PushStreamEntryResponse, error) {
	if err := req.Context.Validate(); err != nil {
		return nil, err
	}
	// todo: more checking here.
	if req.Entry == nil {
		return nil, errors.New("Entry must be specified.")
	}

	var jsonData map[string]interface{}
	if err := json.Unmarshal([]byte(req.Entry.JsonData), &jsonData); err != nil {
		return nil, err
	}

	stateId := historian.StreamTableName(req.Context.HostIdentifier, req.Context.ComponentId, req.Context.StateId)
	state, err := s.Historian.GetStream(stateId)
	if err != nil {
		return nil, err
	}
	err = state.SaveEntry(&stream.StreamEntry{
		Timestamp: util.NumberToTime(req.Entry.Timestamp),
		Data:      stream.StateData(jsonData),
		Type:      stream.StreamEntryType(req.Entry.EntryType),
	})
	if err != nil {
		return nil, err
	}

	// check remote config
	res := &remote.PushStreamEntryResponse{}
	conf, err := s.Historian.BuildRemoteStreamConfig(req.Context.HostIdentifier)
	if err == nil {
		if conf.Crc32 != req.ConfigCrc32 {
			res.Config = conf
		}
	}
	return res, nil
}

func RegisterServer(server *grpc.Server, rctx *r.Session, historianInstance *historian.Historian) {
	remote.RegisterReporterRemoteServiceServer(server, &HistorianRemoteService{
		Session:   rctx,
		Historian: historianInstance,
	})
}
