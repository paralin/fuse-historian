package service

import (
	"github.com/fuserobotics/historian"
	"github.com/fuserobotics/reporter/remote"

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
	return nil, nil
}

func RegisterServer(server *grpc.Server, rctx *r.Session, historianInstance *historian.Historian) {
	remote.RegisterReporterRemoteServiceServer(server, &HistorianRemoteService{
		Session:   rctx,
		Historian: historianInstance,
	})
}
