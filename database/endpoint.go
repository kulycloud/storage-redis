package database

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/golang/protobuf/jsonpb"
	protoCommon "github.com/kulycloud/protocol/common"
	protoStorage "github.com/kulycloud/protocol/storage"
	"strings"
)

type EndpointType string

const (
	ServiceLBEndpoints EndpointType = "service-lb"
)

func dbEndpointsName(endpointType EndpointType, name *protoStorage.NamespacedName) string {
	return fmt.Sprintf("endpoints/%s/%s:%s", endpointType, name.Namespace, name.Name)
}

func (connector *Connector) SetEndpoints(ctx context.Context, endpointType EndpointType, name *protoStorage.NamespacedName, endpoints *protoCommon.EndpointList) error {
	if endpoints.Endpoints == nil || len(endpoints.Endpoints) == 0 {
		return connector.redisClient.Del(ctx, dbEndpointsName(endpointType, name)).Err()
	}

	m := jsonpb.Marshaler{}
	str, err := m.MarshalToString(endpoints)
	if err != nil {
		return fmt.Errorf("could not serialize json: %w", err)
	}

	return connector.redisClient.Set(ctx, dbEndpointsName(endpointType, name), str, 0).Err()
}

func (connector *Connector) GetEndpoints(ctx context.Context, endpointType EndpointType, name *protoStorage.NamespacedName) (*protoCommon.EndpointList, error) {

	str, err := connector.redisClient.Get(ctx, dbEndpointsName(endpointType, name)).Result()

	el := &protoCommon.EndpointList{}
	if err != nil {
		if err != redis.Nil {
			return nil, err
		}
		el.Endpoints = []*protoCommon.Endpoint {}
	} else {
		err = jsonpb.Unmarshal(strings.NewReader(str), el)
		if err != nil {
			return nil, fmt.Errorf("could not deserialize json: %w", err)
		}
	}

	return el, nil
}
