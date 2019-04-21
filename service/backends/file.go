package backends

import (
	"io/ioutil"
	"net/url"
	"os"
	"path"
)

const (
	FileBackendScheme = "file"
)

type fileBackend struct {
	dirPath string
}

func NewFileBackend(u *url.URL) (*fileBackend, error) {
	dirPath := u.Path
	_, err := os.Stat(dirPath)
	if err != nil && os.IsNotExist(err) {
		err = os.MkdirAll(dirPath, 0777)
	}
	if err != nil {
		return nil, err
	}
	return &fileBackend{dirPath}, nil
}

func (b *fileBackend) Get(objectPath string) ([]byte, error) {
	filePath := path.Join(b.dirPath, objectPath)
	_, err := os.Stat(filePath)
	if err != nil && os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return ioutil.ReadFile(filePath)
}

func (b *fileBackend) Put(objectPath string, object []byte) error {
	filePath := path.Join(b.dirPath, objectPath)
	dirPath := path.Dir(filePath)
	_, err := os.Stat(dirPath)
	if err != nil && os.IsNotExist(err) {
		err = os.MkdirAll(dirPath, 0777)
	}
	if err != nil {
		return err
	}
	return ioutil.WriteFile(filePath, object, 0666)
}
