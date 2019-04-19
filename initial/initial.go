package initial

import (
	"fmt"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/easy-oj/common/logs"
	"github.com/easy-oj/common/proto/oss"
	"github.com/easy-oj/common/settings"
	"github.com/easy-oj/oss/common/redis_client"
	"github.com/easy-oj/oss/service"
)

func Initialize() {
	redis_client.InitRedisClient()

	address := fmt.Sprintf("0.0.0.0:%d", settings.OSS.Port)
	lis, err := net.Listen("tcp", address)
	if err != nil {
		panic(err)
	}
	server := grpc.NewServer()
	oss.RegisterOSSServiceServer(server, service.NewOSSHandler())
	reflection.Register(server)
	go func() {
		if err := server.Serve(lis); err != nil {
			panic(err)
		}
	}()
	logs.Info("[Initialize] service served on %s", address)
}
