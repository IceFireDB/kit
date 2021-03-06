// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package models

import (
	"time"

	"github.com/IceFireDB/kit/pkg/models/client"
	"github.com/IceFireDB/kit/pkg/models/client/etcd"
	etcdclient "github.com/IceFireDB/kit/pkg/models/client/etcdv2"
	zkclient "github.com/IceFireDB/kit/pkg/models/client/zk"
	"github.com/pkg/errors"
)

func NewClient(coordinator string, addrlist string, auth string, timeout time.Duration) (client.Client, error) {
	switch coordinator {
	case "zk", "zookeeper":
		return zkclient.New(addrlist, auth, timeout)
	case "etcdv2":
		return etcdclient.New(addrlist, auth, timeout)
	case "etcd":
		return etcd.New(addrlist, auth, timeout)
	}
	return nil, errors.Errorf("invalid coordinator name = %s", coordinator)
}
