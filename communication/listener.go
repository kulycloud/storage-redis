package communication

import (
	"context"
	"errors"
	"fmt"
	"github.com/kulycloud/common/logging"
	"github.com/kulycloud/protocol/common"
	protoCommon "github.com/kulycloud/protocol/common"
	protoStorage "github.com/kulycloud/protocol/storage"
	"github.com/kulycloud/storage-redis/config"
	"github.com/kulycloud/storage-redis/database"
	"google.golang.org/grpc"
	"net"
)

var _ protoStorage.StorageServer = &Listener{}

var ErrInvalidRequest = errors.New("invalid request")

var logger = logging.GetForComponent("communication")

type Listener struct {
	server *grpc.Server
	listener net.Listener
	dbConnector *database.Connector
}

func NewListener(dbConnector *database.Connector) *Listener {
	return &Listener{
		dbConnector: dbConnector,
	}
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
	var uid string
	switch val := request.Id.(type) {
	case *protoStorage.SetRouteRequest_Uid:
		uid = val.Uid
	case *protoStorage.SetRouteRequest_NamespacedName:
		uid = database.RouteUidFromNamespacedName(val.NamespacedName)
	default:
		return nil, fmt.Errorf("id is invalid: %w", ErrInvalidRequest)
	}

	err := listener.dbConnector.SetRoute(ctx, uid, request.Data)
	if err != nil {
		return nil, fmt.Errorf("could not set route: %w", err)
	}

	return &protoStorage.SetRouteResponse{Uid: uid}, nil
}

func (listener *Listener) GetRoute(ctx context.Context, request *protoStorage.GetRouteRequest) (*protoStorage.GetRouteResponse, error) {
	var uid string
	switch val := request.Id.(type) {
	case *protoStorage.GetRouteRequest_Uid:
		uid = val.Uid
	case *protoStorage.GetRouteRequest_NamespacedName:
		uid = database.RouteUidFromNamespacedName(val.NamespacedName)
	default:
		return nil, fmt.Errorf("id is invalid: %w", ErrInvalidRequest)
	}

	route := &protoStorage.Route{}
	err := listener.dbConnector.GetRoute(ctx, uid, route)
	if err != nil {
		return nil, fmt.Errorf("could not get route: %w", err)
	}

	return &protoStorage.GetRouteResponse{Route: &protoStorage.RouteWithId{Uid: uid, Route: route}}, nil
}

func (listener *Listener) GetRouteStep(ctx context.Context, request *protoStorage.GetRouteStepRequest) (*protoStorage.GetRouteStepResponse, error) {
	var uid string
	switch val := request.Id.(type) {
	case *protoStorage.GetRouteStepRequest_Uid:
		uid = val.Uid
	case *protoStorage.GetRouteStepRequest_NamespacedName:
		uid = database.RouteUidFromNamespacedName(val.NamespacedName)
	default:
		return nil, fmt.Errorf("id is invalid: %w", ErrInvalidRequest)
	}

	step := &protoStorage.RouteStep{}
	err := listener.dbConnector.GetRouteStep(ctx, uid, request.StepId, step)
	if err != nil {
		return nil, fmt.Errorf("could not get step: %w", err)
	}

	return &protoStorage.GetRouteStepResponse{Step: step}, nil
}
