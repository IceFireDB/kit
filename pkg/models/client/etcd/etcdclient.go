package etcd

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/CodisLabs/codis/pkg/utils/errors"
	log "github.com/IceFireDB/kit/pkg/logger"
	clientlocal "github.com/IceFireDB/kit/pkg/models/client"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const MAX_TTL = 365 * 24 * 60 * 60 * time.Second

var ErrClosedClient = errors.New("use of closed etcd client")

var (
	ErrNotDir   = errors.New("etcd: not a dir")
	ErrNotFile  = errors.New("etcd: not a file")
	ErrNotExist = errors.New("etcd: not exist")
)

type Client struct {
	sync.Mutex
	client *clientv3.Client

	closed  bool
	timeout time.Duration
	lastKey int

	cancel  context.CancelFunc
	context context.Context
}

func New(addrlist string, auth string, timeout time.Duration) (*Client, error) {
	endpoints := strings.Split(addrlist, ",")
	for i, s := range endpoints {
		if s != "" && !strings.HasPrefix(s, "http://") {
			endpoints[i] = "http://" + s
		}
	}
	if timeout <= 0 {
		timeout = time.Second * 5
	}

	config := clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	}

	if auth != "" {
		split := strings.SplitN(auth, ":", 2)
		if len(split) != 2 || split[0] == "" {
			return nil, errors.Errorf("invalid auth")
		}
		config.Username = split[0]
		config.Password = split[1]
	}

	cli, err := clientv3.New(config)
	// etcd clientv3 >= v3.2.10, grpc/grpc-go >= v1.7.3
	if err == context.DeadlineExceeded {
		// handle errors
	}
	if err != nil {
		return nil, errors.Trace(err)
	}

	client := &Client{
		client: cli, timeout: timeout,
	}
	client.context, client.cancel = context.WithCancel(context.Background())
	return client, nil
}

func (c *Client) Close() error {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	c.cancel()
	return nil
}

func (c *Client) newContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(c.context, c.timeout)
}

func isErrNoNode(err error) bool {
	if err != nil {
		switch err {
		case context.Canceled:
		case context.DeadlineExceeded:
		case rpctypes.ErrEmptyKey:
			return false
		default:
			return true
		}
	}
	return false
}

func isErrNodeExists(err error) bool {
	if err != nil {
		if err == context.Canceled {
			// ctx is canceled by another routine
		} else if err == context.DeadlineExceeded {
			// ctx is attached with a deadline and it exceeded
		} else if err == rpctypes.ErrEmptyKey {
			// client-side error: key is not provided
		} else if ev, ok := status.FromError(err); ok {
			code := ev.Code()
			if code == codes.DeadlineExceeded {
				// server-side context might have timed-out first (due to clock skew)
				// while original client-side context is not timed-out yet
			}
		} else {
			// bad cluster endpoints, which are not etcd servers
			return true
		}
	}
	return false
}

func (c *Client) Mkdir(path string) error {
	return nil
	//c.Lock()
	//defer c.Unlock()
	//if c.closed {
	//	return errors.Trace(ErrClosedClient)
	//}
	//log.Debugf("etcd mkdir node %s", path)
	//cntx, cancel := c.newContext()
	//defer cancel()
	//_, err := c.client.Put(cntx, path, "", &clientv3.SetOptions{Dir: true, PrevExist: clientv3.PrevNoExist})
	//if err != nil && !isErrNodeExists(err) {
	//	log.Debugf("etcd mkdir node %s failed: %s", path, err)
	//	return errors.Trace(err)
	//}
	//log.Debugf("etcd mkdir OK")
	//return nil
}

func (c *Client) Create(path string, data []byte) error {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return errors.Trace(ErrClosedClient)
	}
	cntx, cancel := c.newContext()
	defer cancel()
	log.Debugf("etcd create node %s", path)
	_, err := c.client.Put(cntx, path, string(data)) //&clientv3.OpOption{PrevExist: clientv3.PrevNoExist})
	if err != nil {
		log.Debugf("etcd create node %s failed: %s", path, err)
		return errors.Trace(err)
	}
	log.Debugf("etcd create OK")
	return nil
}

func (c *Client) Update(path string, data []byte) error {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return errors.Trace(ErrClosedClient)
	}
	cntx, cancel := c.newContext()
	defer cancel()
	log.Debugf("etcd update node %s", path)
	_, err := c.client.Put(cntx, path, string(data))
	if err != nil {
		log.Debugf("etcd update node %s failed: %s", path, err)
		return errors.Trace(err)
	}
	log.Debugf("etcd update OK")
	return nil
}

func (c *Client) Delete(path string) error {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return errors.Trace(ErrClosedClient)
	}
	cntx, cancel := c.newContext()
	defer cancel()
	log.Debugf("etcd delete node %s", path)
	res, err := c.client.Delete(cntx, path)
	if err != nil {
		log.Debugf("etcd delete node %s failed: %s", path, err)
		return errors.Trace(err)
	}
	log.Debugf("etcd delete OK %d", res.Deleted)
	return nil
}

func (c *Client) Read(path string, must bool) ([]byte, error) {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return nil, errors.Trace(ErrClosedClient)
	}
	cntx, cancel := c.newContext()
	defer cancel()
	r, err := c.client.Get(cntx, path)
	switch {
	case err != nil:
		log.Debugf("etcd read node %s failed: %s", path, err)
		return nil, errors.Trace(err)
	case r.Count > 1:
		log.Debugf("etcd read node %s failed: not a file", path)
		return nil, errors.Trace(ErrNotFile)
	case r.Count == 1:
		return r.Kvs[0].Value, nil
	default:
		if !must {
			return nil, nil
		}
		log.Debugf("etcd read node %s failed: not a file", path)
		return nil, errors.Trace(ErrNotFile)
	}
}

func (c *Client) List(path string, must bool) ([]string, error) {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return nil, errors.Trace(ErrClosedClient)
	}
	if path[len(path)-1] != '/' {
		path += "/"
	}
	cntx, cancel := c.newContext()
	defer cancel()
	r, err := c.client.Get(cntx, path, clientv3.WithPrefix(), clientv3.WithKeysOnly())
	switch {
	case err != nil:
		log.Debugf("etcd list node %s failed: %s", path, err)
		return nil, errors.Trace(err)
	case r.Count == 0:
		if !must {
			return nil, nil
		}
		log.Debugf("etcd list node %s failed: not a dir", path)
		return nil, errors.Trace(ErrNotDir)
	default:
		paths := make([]string, 0, r.Count)
		for _, node := range r.Kvs {
			paths = append(paths, string(node.Key))
		}
		return paths, nil
	}
}

// assume this is only used by cli, and cli operation is locked. So just not support concurrency create.
func (c *Client) CreateInOrder(path string, data []byte) (string, error) {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return "", errors.Trace(ErrClosedClient)
	}
	cntx, cancel := c.newContext()
	defer cancel()
	log.Debugf("etcd create node %s", path)
	if path[len(path)-1] != '/' {
		path += "/"
	}
	if c.lastKey == 0 {
		getoptions := []clientv3.OpOption{clientv3.WithPrefix(), clientv3.WithKeysOnly()}
		getoptions = append(getoptions, clientv3.WithLastKey()...)
		last, err := c.client.Get(cntx, path, getoptions...)
		if err != nil {
			log.Debugf("etcd get last node %s failed: %s", path, err)
			return "", errors.Trace(err)
		}
		if last.Count != 0 {
			lastkey := last.Kvs[0].Key
			paths := strings.Split(string(lastkey), "/")
			c.lastKey, err = strconv.Atoi(paths[len(paths)-1])
			if err != nil {
				log.Debugf("etcd get last node %s parse key %s failed: %s", path, string(lastkey), err)
				return "", errors.Trace(err)
			}
		}
	}
	c.lastKey++
	key := fmt.Sprintf("%06d", c.lastKey)
	path = path + key

	_, err := c.client.Put(cntx, path, string(data))
	if err != nil {
		log.Debugf("etcd create node %s failed: %s", path, err)
		return "", errors.Trace(err)
	}
	log.Debugf("etcd create OK")
	return path, nil
}

func (c *Client) WatchInOrder(path string) (<-chan clientlocal.Event, []string, error) {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return nil, nil, errors.Trace(ErrClosedClient)
	}
	if path[len(path)-1] != '/' {
		path += "/"
	}
	log.Debugf("etcd watch-inorder node %s", path)
	cntx, cancel := c.newContext()
	defer cancel()
	r, err := c.client.Get(cntx, path, clientv3.WithPrefix(), clientv3.WithKeysOnly())
	switch {
	case err != nil:
		log.Debugf("etcd watch-inorder node %s failed: %s", path, err)
		return nil, nil, errors.Trace(err)
	}
	var paths []string
	for _, node := range r.Kvs {
		paths = append(paths, string(node.Key))
	}
	signal := make(chan clientlocal.Event, 1)
	go func() {
		var et clientlocal.EventType = clientlocal.EventNotWatching
		cntx, cancel := context.WithCancel(c.context)
		defer func() {
			cancel()
			signal <- clientlocal.Event{Type: et}
			close(signal)
		}()
		watch := c.client.Watch(cntx, path, clientv3.WithPrefix(), clientv3.WithFilterDelete())
		for {
			r, ok := <-watch
			switch {
			case !ok:
				log.Debugf("etch watch-inorder node %s canceled", path)
				return
			case !r.Created:
				et = clientlocal.EventNodeChildrenChanged
				log.Debugf("etcd watch-inorder node %s update", path)
				return
			}
			log.Debugf("etch watch-inorder node %s ignore", path)
		}
	}()
	log.Debugf("etcd watch-inorder OK")
	return signal, paths, nil
}
