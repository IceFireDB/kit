/**
 * @Author: zhangchao
 * @Description:
 * @Date: 2021/9/28 5:19 下午
 */
package models

import (
	"path"
	"strconv"
	"testing"
	"time"

	log "github.com/IceFireDB/kit/pkg/logger"

	"github.com/IceFireDB/kit/pkg/models/client"

	"github.com/stretchr/testify/assert"

	"github.com/IceFireDB/kit/pkg/models/client/etcd"
)

var (
	testByte  = []byte(`{"data":"data"}`)
	testByte2 = []byte(`{"data2":"data2"}`)
)

func init() {
	log.Init("test")
}

func getClient() client.Client {
	client, err := etcd.New("47.117.125.229:2379", "", time.Second*5000)
	// client, err := NewClient("zookeeper", "localhost:2181", "", time.Second*5)
	if err != nil {
		panic(err)
	}
	return client
}

func TestSet(t *testing.T) {
	path := "/test/a/b"
	client := getClient()
	err := client.Create("/test/a/b", testByte)
	err = client.Create("/test/a/b/e", testByte)
	err = client.Create("/test/a/be", testByte)
	assert.Nil(t, err)
	err = client.Update(path, testByte2)
	assert.Nil(t, err)
	d, err := client.Read(path, true)
	assert.Nil(t, err)
	assert.Equal(t, d, testByte2)
}

func TestGet(t *testing.T) {
	base := "/test/a/c/ppppp"
	client := getClient()
	err := client.Create(base, testByte)
	assert.Nil(t, err)
	d, err := client.Read(base, true)
	assert.Nil(t, err)
	assert.Equal(t, d, testByte)
	err = client.Delete(base)
	assert.Nil(t, err)
	_, err = client.Read(base, true)
	assert.NotNil(t, err)
}

func TestList(t *testing.T) {
	var err error
	base := "/test/list"
	client := getClient()
	for i := 0; i < 10; i++ {
		err = client.Create(path.Join(base, strconv.Itoa(i)), testByte)
		assert.Nil(t, err)
	}
	d, err := client.List(base, true)
	assert.Nil(t, err)
	assert.Equal(t, len(d), 10)
	for i := 0; i < 10; i++ {
		assert.Equal(t, d[i], path.Join(base, strconv.Itoa(i)))
	}
	// delete not with prefix, so cannot delete path
	err = client.Delete(base)
	assert.Nil(t, err)
	d, err = client.List(base, false)
	assert.Nil(t, err)
	assert.Equal(t, len(d), 10)
}
