package communication

import (
	"context"
	"errors"
	"fmt"
	commonCommunication "github.com/kulycloud/common/communication"
	"github.com/kulycloud/common/logging"
	protoCommon "github.com/kulycloud/protocol/common"
	protoStorage "github.com/kulycloud/protocol/storage"
	"github.com/kulycloud/storage-redis/config"
	"github.com/kulycloud/storage-redis/database"
)

var ControlPlane *commonCommunication.ControlPlaneCommunicator

var ErrInvalidRequest = errors.New("invalid request")

var logger = logging.GetForComponent("communication")

var _ protoStorage.StorageServer = &StorageHandler{}
type StorageHandler struct {
	protoStorage.UnimplementedStorageServer
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
	uid, err := handler.dbConnector.SetRoute(ctx, request.NamespacedName, request.Data)
	if err != nil {
		return nil, fmt.Errorf("could not set route: %w", err)
	}

	return &protoStorage.SetRouteResponse{Uid: uid}, nil
}

func (handler *StorageHandler) GetRoute(ctx context.Context, request *protoStorage.GetRouteRequest) (*protoStorage.GetRouteResponse, error) {
	var uid string
	namespacedName := &protoStorage.NamespacedName{}

	switch val := request.Id.(type) {
	case *protoStorage.GetRouteRequest_Uid:
		uid = val.Uid
		var err error
		namespacedName, err = database.ParseUid(uid)
		if err != nil {
			return nil, err
		}
	case *protoStorage.GetRouteRequest_NamespacedName:
		var err error
		uid, err = handler.dbConnector.GetRouteUidLatestRevision(ctx, val.NamespacedName)
		namespacedName = val.NamespacedName
		if err != nil {
			return nil, fmt.Errorf("route not found: %w", ErrInvalidRequest)
		}
	default:
		return nil, fmt.Errorf("id is invalid: %w", ErrInvalidRequest)
	}

	route := &protoStorage.Route{}
	err := handler.dbConnector.GetRoute(ctx, uid, route)
	if err != nil {
		return nil, fmt.Errorf("could not get route: %w", err)
	}

	return &protoStorage.GetRouteResponse{Route: &protoStorage.RouteWithId{Uid: uid, Route: route, Name: namespacedName}}, nil
}

func (handler *StorageHandler) GetRouteStep(ctx context.Context, request *protoStorage.GetRouteStepRequest) (*protoStorage.GetRouteStepResponse, error) {
	var uid string
	switch val := request.Id.(type) {
	case *protoStorage.GetRouteStepRequest_Uid:
		uid = val.Uid
	case *protoStorage.GetRouteStepRequest_NamespacedName:
		var err error
		uid, err = handler.dbConnector.GetRouteUidLatestRevision(ctx, val.NamespacedName)
		if err != nil {
			return nil, fmt.Errorf("route not found: %w", ErrInvalidRequest)
		}
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

func (handler *StorageHandler) GetPopulatedRouteStep(ctx context.Context, request *protoStorage.GetRouteStepRequest) (*protoStorage.GetPopulatedRouteStepResponse, error) {
	var uid string
	switch val := request.Id.(type) {
	case *protoStorage.GetRouteStepRequest_Uid:
		uid = val.Uid
	case *protoStorage.GetRouteStepRequest_NamespacedName:
		var err error
		uid, err = handler.dbConnector.GetRouteUidLatestRevision(ctx, val.NamespacedName)
		if err != nil {
			return nil, fmt.Errorf("route not found: %w", ErrInvalidRequest)
		}
	default:
		return nil, fmt.Errorf("id is invalid: %w", ErrInvalidRequest)
	}

	step := &protoStorage.RouteStep{}
	err := handler.dbConnector.GetRouteStep(ctx, uid, request.StepId, step)
	if err != nil {
		return nil, fmt.Errorf("could not get step: %w", err)
	}

	populatedStep := &protoStorage.PopulatedRouteStep{
		Service:    step.Service,
		Config:     step.Config,
		Name: 		step.Name,
		References: make(map[string]*protoStorage.PopulatedRouteStepReference),
	}

	routeStep := &protoStorage.RouteStep{}

	for name, stepId := range step.References {
		err = handler.dbConnector.GetRouteStep(ctx, uid, stepId, routeStep)
		if err != nil {
			return nil, err
		}

		endpoints, err := handler.dbConnector.GetEndpoints(ctx, database.ServiceLBEndpoints, routeStep.Service)
		if err != nil {
			return nil, err
		}

		populatedStep.References[name] = &protoStorage.PopulatedRouteStepReference{
			Step: stepId,
			Endpoints: endpoints.Endpoints,
		}
	}

	return &protoStorage.GetPopulatedRouteStepResponse{Step: populatedStep}, nil
}

func (handler *StorageHandler) GetRouteStart(ctx context.Context, request *protoStorage.GetRouteStartRequest) (*protoStorage.GetRouteStartResponse, error) {
	uid, err := handler.dbConnector.GetRouteUidByHost(ctx, request.Host)

	if err != nil {
		return nil, fmt.Errorf("could not get route by host: %w", err)
	}

	step := protoStorage.RouteStep{}
	err = handler.dbConnector.GetRouteStep(ctx, uid, 0, &step)

	if err != nil {
		return nil, fmt.Errorf("could not get route by host: %w", err)
	}

	endpoints, err := handler.dbConnector.GetEndpoints(ctx, database.ServiceLBEndpoints, step.Service)
	if err != nil {
		return nil, fmt.Errorf("error fetching endpoints for route: %w", err)
	}

	return &protoStorage.GetRouteStartResponse{
		Step:      &protoStorage.PopulatedRouteStepReference{
			Step:      0,
			Endpoints: endpoints.Endpoints,
		},
		Uid:       uid,
	}, nil
}

func (handler *StorageHandler) GetRoutesInNamespace(ctx context.Context, request *protoStorage.GetRoutesInNamespaceRequest) (*protoStorage.GetRoutesInNamespaceResponse, error) {
	routes, err := handler.dbConnector.GetRoutesInNamespace(ctx, request.Namespace)

	if err != nil {
		return nil, fmt.Errorf("could not get routes: %w", err)
	}

	return &protoStorage.GetRoutesInNamespaceResponse{
		RouteUids: routes,
	}, nil
}

func (handler *StorageHandler) DeleteRoute(ctx context.Context, request *protoStorage.DeleteRouteRequest) (*protoCommon.Empty, error) {
	return &protoCommon.Empty{}, handler.dbConnector.DeleteRoute(ctx, request.NamespacedName)
}

func (handler *StorageHandler) SetService(ctx context.Context, request *protoStorage.SetServiceRequest) (*protoCommon.Empty, error) {
	err := handler.dbConnector.SetService(ctx, request.NamespacedName, request.Service)
	if err != nil {
		return nil, fmt.Errorf("could not set service: %w", err)
	}

	return &protoCommon.Empty{}, nil
}

func (handler *StorageHandler) GetService(ctx context.Context, request *protoStorage.GetServiceRequest) (*protoStorage.GetServiceResponse, error) {
	var service = &protoStorage.Service{}
	err := handler.dbConnector.GetService(ctx, request.NamespacedName, service)

	if err != nil {
		return nil, fmt.Errorf("could not get service: %w", err)
	}

	return &protoStorage.GetServiceResponse{Service: service}, nil
}

func (handler *StorageHandler) GetServicesInNamespace(ctx context.Context, request *protoStorage.GetServicesInNamespaceRequest) (*protoStorage.GetServicesInNamespaceResponse, error) {
	routes, err := handler.dbConnector.GetServicesInNamespace(ctx, request.Namespace)

	if err != nil {
		return nil, fmt.Errorf("could not get services: %w", err)
	}

	return &protoStorage.GetServicesInNamespaceResponse{
		Names: routes,
	}, nil
}

func (handler *StorageHandler) GetServiceLBEndpoints(ctx context.Context, name *protoStorage.NamespacedName) (*protoCommon.EndpointList, error) {
	return handler.dbConnector.GetEndpoints(ctx, database.ServiceLBEndpoints, name)
}

func (handler *StorageHandler) SetServiceLBEndpoints(ctx context.Context, request *protoStorage.SetServiceLBEndpointsRequest) (*protoCommon.Empty, error) {
	return &protoCommon.Empty{}, handler.dbConnector.SetEndpoints(ctx, database.ServiceLBEndpoints, request.ServiceName, &protoCommon.EndpointList{Endpoints: request.Endpoints})
}

func (handler *StorageHandler) DeleteService(ctx context.Context, request *protoStorage.DeleteServiceRequest) (*protoCommon.Empty, error) {
	return &protoCommon.Empty{}, handler.dbConnector.DeleteService(ctx, request.NamespacedName)
}

func (handler *StorageHandler) GetNamespaces(ctx context.Context, _ *protoCommon.Empty) (*protoStorage.NamespaceList, error) {
	namespaces, err := handler.dbConnector.GetNamespaces(ctx)
	if err != nil {
		return nil, err
	}

	return &protoStorage.NamespaceList{Namespaces: namespaces}, nil
}

func RegisterToControlPlane(dbConnector *database.Connector) {
	communicator := commonCommunication.RegisterToControlPlane("storage",
		config.GlobalConfig.Host, config.GlobalConfig.Port,
		config.GlobalConfig.ControlPlaneHost, config.GlobalConfig.ControlPlanePort, false)

	logger.Info("Starting listener")
	listener := commonCommunication.NewListener(logging.GetForComponent("listener"))
	if err := listener.Setup(config.GlobalConfig.Port); err != nil {
		logger.Panicw("error initializing listener", "error", err)
	}

	handler := NewStorageHandler(dbConnector)
	handler.Register(listener)

	serveErr := listener.Serve()
	ControlPlane = <-communicator

	err := <-serveErr
	if err != nil {
		logger.Panicw("error serving listener", "error", err)
	}
}
