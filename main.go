package main

import (
	"github.com/kulycloud/common/logging"
	"github.com/kulycloud/storage-redis/communication"
	"github.com/kulycloud/storage-redis/config"
	"github.com/kulycloud/storage-redis/database"
	"time"
)

var logger = logging.GetForComponent("init")

func main() {
	defer logging.Sync()

	err := config.ParseConfig()
	if err != nil {
		logger.Fatalw("Error parsing config", "error", err)
	}
	logger.Infow("Finished parsing config", "config", config.GlobalConfig)

	dbConnector := database.NewConnector()
	for {
		err := dbConnector.Connect()
		if err == nil {
			break
		}

		logger.Errorw("Could not connect dbConnector", "error", err)
		logger.Info("Retrying in 5s...")
		time.Sleep(5*time.Second)
	}

	communication.RegisterToControlPlane(dbConnector)
}
