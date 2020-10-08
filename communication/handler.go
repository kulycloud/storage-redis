package communication

import (
	"context"
	"errors"
	"fmt"
	commonCommunication "github.com/kulycloud/common/communication"
	"github.com/kulycloud/common/logging"
	protoStorage "github.com/kulycloud/protocol/storage"
	"github.com/kulycloud/storage-redis/database"
)

var ErrInvalidRequest = errors.New("invalid request")

var logger = logging.GetForComponent("communication")

var _ protoStorage.StorageServer = &StorageHandler{}
type StorageHandler struct {
	dbConnector *database.Connector
}

func NewStorageHandler(dbConnector *database.Connector) *StorageHandler {
	return &StorageHandler{
		dbConnector: dbConnector,
	}
}

func (handler *StorageHandler) Register(listener *commonCommunication.Listener) {
	protoStorage.RegisterStorageServer(listener.Server, handler)
}

func (handler *StorageHandler) SetRoute(ctx context.Context, request *protoStorage.SetRouteRequest) (*protoStorage.SetRouteResponse, error) {
	var uid string
	switch val := request.Id.(type) {
	case *protoStorage.SetRouteRequest_Uid:
		uid = val.Uid
	case *protoStorage.SetRouteRequest_NamespacedName:
		uid = database.RouteUidFromNamespacedName(val.NamespacedName)
	default:
		return nil, fmt.Errorf("id is invalid: %w", ErrInvalidRequest)
	}

	err := handler.dbConnector.SetRoute(ctx, uid, request.Data)
	if err != nil {
		return nil, fmt.Errorf("could not set route: %w", err)
	}

	return &protoStorage.SetRouteResponse{Uid: uid}, nil
}

func (handler *StorageHandler) GetRoute(ctx context.Context, request *protoStorage.GetRouteRequest) (*protoStorage.GetRouteResponse, error) {
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
	err := handler.dbConnector.GetRoute(ctx, uid, route)
	if err != nil {
		return nil, fmt.Errorf("could not get route: %w", err)
	}

	return &protoStorage.GetRouteResponse{Route: &protoStorage.RouteWithId{Uid: uid, Route: route}}, nil
}

func (handler *StorageHandler) GetRouteStep(ctx context.Context, request *protoStorage.GetRouteStepRequest) (*protoStorage.GetRouteStepResponse, error) {
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
	err := handler.dbConnector.GetRouteStep(ctx, uid, request.StepId, step)
	if err != nil {
		return nil, fmt.Errorf("could not get step: %w", err)
	}

	return &protoStorage.GetRouteStepResponse{Step: step}, nil
}
