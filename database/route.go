package database

import (
	"context"
	"encoding/json"
	"github.com/go-redis/redis/v8"
	"github.com/golang/protobuf/jsonpb"
	protoStorage "github.com/kulycloud/protocol/storage"
	"strings"
)

type dbRoute struct {
	Host string `json:"host"`
	// more to follow
}

func dbRouteFromProtoRoute(route *protoStorage.Route) *dbRoute {
	return &dbRoute{
		Host: route.Host,
	}
}

func unpackDbRoute(dbRoute *dbRoute, route *protoStorage.Route) {
	route.Host = dbRoute.Host
}

func dbRouteName(uid string) string {
	return "routes/" + uid
}

func dbRouteStepsName(uid string) string {
	return "routes/" + uid + "/steps"
}

func RouteUidFromNamespacedName(namespacedName *protoStorage.NamespacedName) string {
	return namespacedName.Namespace + ":" + namespacedName.Name
}

func (connector *Connector) SetRoute(ctx context.Context, uid string, route *protoStorage.Route) error {
	// First update parent object
	dbRoute := dbRouteFromProtoRoute(route)
	str, err := json.Marshal(dbRoute)
	if err != nil {
		return err
	}


	p := connector.redisClient.TxPipeline()
	p.Set(ctx, dbRouteName(uid), str, 0)
	p.Del(ctx, dbRouteStepsName(uid))


	m := jsonpb.Marshaler{}
	for _, step := range route.Steps {
		stepStr, err := m.MarshalToString(step)
		if err != nil {
			return err
		}

		p.RPush(ctx, dbRouteStepsName(uid), stepStr)
	}

	_, err = p.Exec(ctx)
	return err
}

func (connector *Connector) GetRoute(ctx context.Context, uid string, route *protoStorage.Route) error {
	routeJson, err := connector.redisClient.Get(ctx, dbRouteName(uid)).Result()
	if err != nil {
		if err == redis.Nil {
			return ErrorNotFound
		}
		return err
	}

	dbRoute := &dbRoute{}
	err = json.Unmarshal([]byte(routeJson), dbRoute)
	if err != nil {
		return err
	}

	unpackDbRoute(dbRoute, route)

	op := connector.redisClient.LRange(ctx, dbRouteStepsName(uid), 0, -1)
	if op.Err() != nil {
		return op.Err()
	}

	route.Steps = make([]*protoStorage.RouteStep, 0)
	for _, stepJson := range op.Val() {
		step := &protoStorage.RouteStep{}
		err = jsonpb.Unmarshal(strings.NewReader(stepJson), step)
		if err != nil {
			return err
		}
		route.Steps = append(route.Steps, step)
	}

	return nil
}

func (connector *Connector) GetRouteStep(ctx context.Context, uid string, id uint32, step *protoStorage.RouteStep) error {

	op := connector.redisClient.LRange(ctx, dbRouteStepsName(uid), 0, -1)
	if op.Err() != nil {
		return op.Err()
	}

	stepJson, err := connector.redisClient.LIndex(ctx, dbRouteStepsName(uid), int64(id)).Result()
	if err != nil {
		if err == redis.Nil {
			return ErrorNotFound
		}
		return err
	}
	return jsonpb.Unmarshal(strings.NewReader(stepJson), step)
}
