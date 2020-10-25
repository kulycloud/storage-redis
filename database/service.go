package database

import (
	"context"
	"github.com/go-redis/redis/v8"
	"github.com/golang/protobuf/jsonpb"
	protoStorage "github.com/kulycloud/protocol/storage"
	"strings"
)

func dbServiceName(namespacedName *protoStorage.NamespacedName) string {
	return "services/" + namespacedName.Namespace + ":" + namespacedName.Name
}

func dbNamespaceServicesName(namespace string) string {
	return "services/" + namespace
}

func (connector *Connector) SetService(ctx context.Context, namespacedName *protoStorage.NamespacedName, service *protoStorage.Service) error {
	m := jsonpb.Marshaler{}
	serviceStr, err := m.MarshalToString(service)
	if err != nil {
		return err
	}
	// First update parent object

	p := connector.redisClient.TxPipeline()
	p.Set(ctx, dbServiceName(namespacedName), serviceStr, 0)
	p.SAdd(ctx, dbNamespaceServicesName(namespacedName.Namespace), namespacedName.Name)

	_, err = p.Exec(ctx)
	return err
}

func (connector *Connector) GetService(ctx context.Context, name *protoStorage.NamespacedName, service *protoStorage.Service) error {
	serviceJson, err := connector.redisClient.Get(ctx, dbServiceName(name)).Result()
	if err != nil {
		if err == redis.Nil {
			return ErrorNotFound
		}
		return err
	}

	return jsonpb.Unmarshal(strings.NewReader(serviceJson), service)
}

func (connector *Connector) GetServicesInNamespace(ctx context.Context, namespace string) ([]string, error) {
	return connector.redisClient.SMembers(ctx, dbNamespaceServicesName(namespace)).Result()
}
