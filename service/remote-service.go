package service

import (
	"github.com/fuserobotics/reporter/remote"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

type HistorianRemoteService struct {
}

func (s *HistorianRemoteService) GetRemoteConfig(c context.Context, req *remote.GetRemoteConfigRequest) (*remote.GetRemoteConfigResponse, error) {
	return nil, nil
}

func (s *HistorianRemoteService) PushStreamEntry(c context.Context, req *remote.PushStreamEntryRequest) (*remote.PushStreamEntryResponse, error) {
	return nil, nil
}

var serviceTypeAssertion remote.ReporterRemoteServiceServer = &HistorianRemoteService{}

func RegisterServer(server *grpc.Server) {
	remote.RegisterReporterRemoteServiceServer(server, &HistorianRemoteService{})
}
