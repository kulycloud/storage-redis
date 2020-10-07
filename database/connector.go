package database

import (
	"context"
	"errors"
	"github.com/go-redis/redis/v8"
	"github.com/kulycloud/common/logging"
	"github.com/kulycloud/storage-redis/config"
)

var logger = logging.GetForComponent("database")

var ErrorNotFound = errors.New("not found")

type Connector struct {
	redisClient *redis.Client
}

func NewConnector() *Connector {
	return &Connector{}
}

func (connector *Connector) Connect() error {
	client  := redis.NewClient(&redis.Options{
		Addr: config.GlobalConfig.RedisHost,
		Password: config.GlobalConfig.RedisPassword,
	})

	_, err := client.Ping(context.TODO()).Result()
	if err != nil {
		return err
	}

	connector.redisClient = client
	logger.Info("Connected to DB")

	return nil
}

func (connector *Connector) GetRoute(ctx context.Context, id string) (string, error) {
	str, err := connector.redisClient.Get(ctx, dbRouteName(id)).Result()
	if err != nil {
		if err == redis.Nil {
			return "", ErrorNotFound
		}
		return "", nil
	}
	return str, nil
}

func (connector *Connector) SetRoute(ctx context.Context, id string, route string) error {
	return connector.redisClient.Set(ctx, dbRouteName(id), route, 0).Err()
}

func dbRouteName(id string) string {
	return "routes/" + id
}
