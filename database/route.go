package database

import (
	"context"
	"encoding/json"
	protoStorage "github.com/kulycloud/protocol/storage"
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


	for _, step := range route.Steps {
		stepStr, err := json.Marshal(step)
		if err != nil {
			return err
		}

		p.RPush(ctx, dbRouteStepsName(uid), stepStr)
	}

	_, err = p.Exec(ctx)
	return err
}
