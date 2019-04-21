package service

import (
	"context"
	"errors"
	"net/url"
	"time"

	"github.com/go-redis/redis"

	"github.com/easy-oj/common/logs"
	"github.com/easy-oj/common/proto/oss"
	"github.com/easy-oj/common/settings"
	"github.com/easy-oj/oss/common/redis_client"
	"github.com/easy-oj/oss/service/backends"
)

type ossHandler struct {
	backend backend
}

type backend interface {
	Get(string) ([]byte, error)
	Put(string, []byte) error
}

var (
	unknownBackendSchemeError = errors.New("unknown backend scheme")
)

func NewOSSHandler() *ossHandler {
	u, err := url.Parse(settings.OSS.Backend)
	if err != nil {
		panic(err)
	}
	var b backend
	switch u.Scheme {
	case backends.FileBackendScheme:
		b, err = backends.NewFileBackend(u)
	case backends.OSSBackendScheme:
		b, err = backends.NewOSSBackend(u)
	default:
		err = unknownBackendSchemeError
	}
	if err != nil {
		panic(err)
	}
	return &ossHandler{b}
}

func (h *ossHandler) GetObject(ctx context.Context, req *oss.GetObjectReq) (*oss.GetObjectResp, error) {
	resp := oss.NewGetObjectResp()
	if bs, err := redis_client.Client.Get(req.Path).Bytes(); err != nil {
		if err != redis.Nil {
			logs.Warn("[OSS] GetRedis '%s' error: %s", err.Error())
		}
	} else {
		resp.Object = bs
		return resp, nil
	}
	if object, err := h.backend.Get(req.Path); err != nil {
		logs.Error("[OSS] GetObject '%s' error: %s", req.Path, err.Error())
		return resp, err
	} else if object == nil {
		logs.Warn("[OSS] GetObject '%s' not found", req.Path)
	} else {
		logs.Info("[OSS] GetObject '%s' length = %d", req.Path, len(object))
		resp.Object = object
		if err := redis_client.Client.Set(req.Path, object, time.Hour).Err(); err != nil {
			logs.Warn("[OSS] SetRedis '%s' error: %s", err.Error())
		}
	}
	return resp, nil
}

func (h *ossHandler) PutObject(ctx context.Context, req *oss.PutObjectReq) (*oss.PutObjectResp, error) {
	resp := oss.NewPutObjectResp()
	if err := h.backend.Put(req.Path, req.Object); err != nil {
		logs.Error("[OSS] PutObject '%s' error: %s", req.Path, err.Error())
		return resp, err
	} else {
		logs.Info("[OSS] PutObject '%s' length = %d", req.Path, len(req.Object))
		if err := redis_client.Client.Set(req.Path, req.Object, time.Hour).Err(); err != nil {
			logs.Warn("[OSS] SetRedis '%s' error: %s", err.Error())
		}
	}
	return resp, nil
}
