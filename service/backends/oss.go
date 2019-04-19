package backends

import (
	"bytes"
	"errors"
	"io/ioutil"
	"net/url"
	"strings"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"

	"github.com/easy-oj/common/logs"
)

const (
	OSSBackendScheme = "oss"
)

type ossBackend struct {
	*oss.Bucket
}

var (
	authorizationRequiredError = errors.New("authorization required")
	malformedHostError         = errors.New("malformed host")
)

func NewOSSBackend(u *url.URL) (*ossBackend, error) {
	secret, ok := u.User.Password()
	if !ok {
		return nil, authorizationRequiredError
	}
	ss := strings.Split(u.Host, ".")
	if len(ss) != 4 {
		return nil, malformedHostError
	}
	endpoint := strings.TrimPrefix(u.Host, ss[0]+".")
	if client, err := oss.New(endpoint, u.User.Username(), secret); err != nil {
		return nil, err
	} else if bucket, err := client.Bucket(ss[0]); err != nil {
		return nil, err
	} else {
		logs.Info("[OSSBackend] endpoint = %s, bucket = %s", endpoint, ss[0])
		return &ossBackend{bucket}, nil
	}
}

func (b *ossBackend) Get(path string) ([]byte, error) {
	if ok, err := b.IsObjectExist(path); err != nil {
		return nil, err
	} else if !ok {
		return nil, nil
	}
	reader, err := b.GetObject(path)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return ioutil.ReadAll(reader)
}

func (b *ossBackend) Put(path string, object []byte) error {
	reader := bytes.NewReader(object)
	return b.PutObject(path, reader)
}
