package database

import (
	"context"
	"github.com/go-redis/redis/v8"
)

const dbNamespacesName = "namespaces"

func (connector *Connector) GetNamespaces(ctx context.Context) ([]string, error) {
	return connector.redisClient.SMembers(ctx, dbNamespacesName).Result()
}

func (connector *Connector) AddNamespaceIfNotExists(ctx context.Context, name string) error {
	return connector.redisClient.SAdd(ctx, dbNamespacesName, name).Err()
}

func (connector *Connector) AddNamespaceIfNotExistsTx(ctx context.Context, tx redis.Pipeliner, name string) {
	tx.SAdd(ctx, dbNamespacesName, name)
}

func (connector *Connector) DeleteNamespace(ctx context.Context, name string) error {
	return connector.redisClient.SRem(ctx, dbNamespacesName, name).Err()
}

func (connector *Connector) ExistsNamespace(ctx context.Context, name string) (bool, error) {
	return connector.redisClient.SIsMember(ctx, dbNamespacesName, name).Result()
}

func (connector *Connector) GetNamespaceSize(ctx context.Context, name string) (int64, error) {
	var size int64 = 0
	services, err := connector.redisClient.SCard(ctx, dbNamespaceServicesName(name)).Result()
	if err != nil {
		size += services
	} else {
		if err != redis.Nil {
			return 0, err
		}
	}

	routes, err := connector.redisClient.SCard(ctx, dbNamespaceRoutesName(name)).Result()
	if err != nil {
		size += routes
	} else {
		if err != redis.Nil {
			return 0, err
		}
	}

	return size, nil
}

func (connector *Connector) DeleteNamespaceIfEmpty(ctx context.Context, name string) error {
	size, err := connector.GetNamespaceSize(ctx, name)
	if err != nil {
		return err
	}

	if size != 0 {
		return nil
	}

	return connector.DeleteNamespace(ctx, name)
}
