package database

import (
	"context"
	"encoding/json"
	"fmt"
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

func dbNamespaceRoutesName(namespace string) string {
	return "routes/" + namespace
}

func dbLatestRevisionName(namespacedName *protoStorage.NamespacedName) string {
	return "revisions/routes/" + namespacedName.Namespace + ":" + namespacedName.Name
}

func buildUid(namespacedName *protoStorage.NamespacedName, revision uint64) string {
	return fmt.Sprintf("%s:%s@%v", namespacedName.Namespace, namespacedName.Name, revision)
}

func dbHostRoute(host string) string {
	return "hosts/" + host
}

func (connector *Connector) GetRouteUidLatestRevision(ctx context.Context, namespacedName *protoStorage.NamespacedName) (string, error) {
	revision, err := connector.GetRouteLatestRevision(ctx, namespacedName)
	if err != nil {
		return "", err
	}
	return buildUid(namespacedName, revision), nil
}

func (connector *Connector) GetRouteLatestRevision(ctx context.Context, namespacedName *protoStorage.NamespacedName) (uint64, error) {
	return connector.redisClient.Get(ctx, dbLatestRevisionName(namespacedName)).Uint64()
}

func (connector *Connector) SetRoute(ctx context.Context, namespacedName *protoStorage.NamespacedName, route *protoStorage.Route) (string, error) {
	// First update parent object
	dbRoute := dbRouteFromProtoRoute(route)
	str, err := json.Marshal(dbRoute)
	if err != nil {
		return "", err
	}

	revision, err := connector.GetRouteLatestRevision(ctx, namespacedName)
	if err != nil {
		if err == redis.Nil {
			revision = 0
		} else {
			return "", err
		}
	}
	revision++
	uid := buildUid(namespacedName, revision)

	oldUid, err := connector.GetRouteUidLatestRevision(ctx, namespacedName)
	hasOldUid := true
	if err != nil {
		if err != redis.Nil {
			return "", err
		} else {
			hasOldUid = false
		}
	}

	p := connector.redisClient.TxPipeline()
	p.Set(ctx, dbRouteName(uid), str, 0)
	p.Del(ctx, dbRouteStepsName(uid))
	p.SAdd(ctx, dbNamespaceRoutesName(namespacedName.Namespace), uid)
	if hasOldUid {
		p.SRem(ctx, dbNamespaceRoutesName(namespacedName.Namespace), oldUid)
	}
	p.Set(ctx, dbHostRoute(route.Host), uid, 0)
	p.Set(ctx, dbLatestRevisionName(namespacedName), revision, 0)

	m := jsonpb.Marshaler{}
	for _, step := range route.Steps {
		stepStr, err := m.MarshalToString(step)
		if err != nil {
			return "", err
		}

		p.RPush(ctx, dbRouteStepsName(uid), stepStr)
	}

	_, err = p.Exec(ctx)
	return uid, err
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

func (connector *Connector) GetRoutesInNamespace(ctx context.Context, namespace string) ([]string, error) {
	return connector.redisClient.SMembers(ctx, dbNamespaceRoutesName(namespace)).Result()
}

func (connector *Connector) GetRouteUidByHost(ctx context.Context, host string) (string, error) {
	res, err := connector.redisClient.Get(ctx, dbHostRoute(host)).Result()
	if err != nil {
		if err == redis.Nil {
			return "", ErrorNotFound
		}
		return "", err
	}

	return res, nil
}
