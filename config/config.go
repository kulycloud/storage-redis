package config

import (
	commonConfig "github.com/kulycloud/common/config"
)

type Config struct {
	Host string `configName:"host"`
	Port uint32 `configName:"port"`
	RedisHost string `configName:"redisHost"`
	RedisPassword string `configName:"redisPassword"`
}

var GlobalConfig = &Config{}

func ParseConfig() error {
	parser := commonConfig.NewParser()
	parser.AddProvider(commonConfig.NewCliParamProvider())
	parser.AddProvider(commonConfig.NewEnvironmentVariableProvider())

	return parser.Populate(GlobalConfig)
}
