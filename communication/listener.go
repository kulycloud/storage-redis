package communication

import (
	"context"
	"fmt"
	"github.com/kulycloud/common/logging"
	"github.com/kulycloud/protocol/common"
	protoCommon "github.com/kulycloud/protocol/common"
	protoStorage "github.com/kulycloud/protocol/storage"
	"github.com/kulycloud/storage-redis/config"
	"google.golang.org/grpc"
	"net"
)

var _ protoStorage.StorageServer = &Listener{}

var logger = logging.GetForComponent("communication")

type Listener struct {
	server *grpc.Server
	listener net.Listener
}

func NewListener() *Listener {
	return &Listener{}
}

func (listener *Listener) Start() error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%v", config.GlobalConfig.Port))
	if err != nil {
		return err
	}
	listener.listener = lis
	listener.server = grpc.NewServer()
	protoStorage.RegisterStorageServer(listener.server, listener)
	logger.Infow("serving", "port", config.GlobalConfig.Port)
	return listener.server.Serve(listener.listener)
}

func (listener *Listener) Ping(ctx context.Context, empty *common.Empty) (*common.Empty, error) {
	return &protoCommon.Empty{}, nil
}

func (listener *Listener) SetRoute(ctx context.Context, request *protoStorage.SetRouteRequest) (*protoStorage.SetRouteResponse, error) {
	panic("implement me")
}

func (listener *Listener) GetRoute(ctx context.Context, request *protoStorage.GetRouteRequest) (*protoStorage.GetRouteResponse, error) {
	panic("implement me")
}

func (listener *Listener) GetRouteStep(ctx context.Context, request *protoStorage.GetRouteStepRequest) (*protoStorage.GetRouteStepResponse, error) {
	panic("implement me")
}
