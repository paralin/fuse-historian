package service

import (
	"google.golang.org/grpc"
	r "gopkg.in/dancannon/gorethink.v2"

	"github.com/fuserobotics/historian"
	"github.com/fuserobotics/reporter/remote"
	"github.com/fuserobotics/reporter/view"
)

func RegisterServer(server *grpc.Server, rctx *r.Session, historianInstance *historian.Historian) {
	remote.RegisterReporterRemoteServiceServer(server, &HistorianRemoteService{
		Session:   rctx,
		Historian: historianInstance,
	})
	view.RegisterReporterServiceServer(server, &HistorianViewService{
		Session:   rctx,
		Historian: historianInstance,
	})
}
