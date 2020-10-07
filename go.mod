module github.com/kulycloud/storage-redis

go 1.15

require (
	github.com/go-redis/redis/v8 v8.2.3
	github.com/kulycloud/common v1.0.0
	github.com/kulycloud/protocol v1.0.0
	google.golang.org/grpc v1.32.0
)

replace github.com/kulycloud/common v1.0.0 => ./../common

replace github.com/kulycloud/protocol v1.0.0 => ./../protocol
