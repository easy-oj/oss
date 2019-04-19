package redis_client

import (
	"fmt"

	"github.com/go-redis/redis"

	"github.com/easy-oj/common/logs"
	"github.com/easy-oj/common/settings"
)

var (
	Client *redis.Client
)

func InitRedisClient() {
	Client = dial(fmt.Sprintf("%s:%d", settings.Redis.Host, settings.Redis.Port))
}

func dial(address string) *redis.Client {
	client := redis.NewClient(&redis.Options{
		Addr: address,
	})
	_, err := client.Ping().Result()
	if err != nil {
		panic(err)
	}
	logs.Info("[RedisClient] dial %s", address)
	return client
}
