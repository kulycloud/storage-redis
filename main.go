package main

import (
	"github.com/kulycloud/common/logging"
	"github.com/kulycloud/storage-redis/communication"
	"github.com/kulycloud/storage-redis/config"
	"github.com/kulycloud/storage-redis/database"
	"time"
)

func main() {
	initLogger := logging.GetForComponent("init")
	defer logging.Sync()

	err := config.ParseConfig()
	if err != nil {
		initLogger.Fatalw("Error parsing config", "error", err)
	}
	initLogger.Infow("Finished parsing config", "config", config.GlobalConfig)

	connector := database.NewConnector()
	for {
		err := connector.Connect()
		if err == nil {
			break
		}

		initLogger.Errorw("Could not connect database", "error", err)
		initLogger.Info("Retrying in 5s...")
		time.Sleep(5*time.Second)
	}

	initLogger.Info("Starting listener")
	listener := communication.NewListener()
	err = listener.Start()
	if err != nil {
		initLogger.Panicw("error initializing listener", "error", err)
	}
}
