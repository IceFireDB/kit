// Copyright 2014 Wandoujia Inc. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package models

import (
	"encoding/json"
	"github.com/juju/errors"
	"io/ioutil"
	"net/http"
)

const (
	PROXY_STATE_ONLINE       = "online"
	PROXY_STATE_OFFLINE      = "offline"
	PROXY_STATE_MARK_OFFLINE = "mark_offline"
)

type ProxyInfo struct {
	Id           string `json:"id"`
	Addr         string `json:"addr"`
	LastEvent    string `json:"last_event"`
	LastEventTs  int64  `json:"last_event_ts"`
	State        string `json:"state"`
	Description  string `json:"description"`
	DebugVarAddr string `json:"debug_var_addr"`
}


func (p *ProxyInfo) Encode() []byte {
	return jsonEncode(p)
}

func (p ProxyInfo) Ops() (int64, error) {
	resp, err := http.Get("http://" + p.DebugVarAddr + "/debug/vars")
	if err != nil {
		return -1, errors.Trace(err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return -1, errors.Trace(err)
	}

	m := make(map[string]interface{})
	err = json.Unmarshal(body, &m)
	if err != nil {
		return -1, errors.Trace(err)
	}

	if v, ok := m["router"]; ok {
		if vv, ok := v.(map[string]interface{})["ops"]; ok {
			return int64(vv.(float64)), nil
		}
	}

	return 0, nil
}

func (p ProxyInfo) DebugVars() (map[string]interface{}, error) {
	resp, err := http.Get("http://" + p.DebugVarAddr + "/debug/vars")
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Trace(err)
	}

	m := make(map[string]interface{})
	err = json.Unmarshal(body, &m)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return m, nil
}


//func ProxyList(zkConn zkhelper.Conn, productName string, filter func(*ProxyInfo) bool) ([]ProxyInfo, error) {
//	ret := make([]ProxyInfo, 0)
//	root := GetProxyPath(productName)
//	proxies, _, err := zkConn.Children(root)
//	if err != nil && !zkhelper.ZkErrorEqual(err, zk.ErrNoNode) {
//		return nil, errors.Trace(err)
//	}
//
//	for _, proxyName := range proxies {
//		pi, err := GetProxyInfo(zkConn, productName, proxyName)
//		if err != nil {
//			return nil, errors.Trace(err)
//		}
//		if filter == nil || filter(pi) == true {
//			ret = append(ret, *pi)
//		}
//	}
//
//	return ret, nil
//}
//
//var ErrUnknownProxyStatus = errors.New("unknown status, should be (online offline)")
//
//func SetProxyStatus(zkConn zkhelper.Conn, productName string, proxyName string, status string) error {
//	p, err := GetProxyInfo(zkConn, productName, proxyName)
//	if err != nil {
//		return errors.Trace(err)
//	}
//
//	if status != PROXY_STATE_ONLINE && status != PROXY_STATE_MARK_OFFLINE && status != PROXY_STATE_OFFLINE {
//		return errors.Errorf("%v, %s", ErrUnknownProxyStatus, status)
//	}
//
//	// check slot status before setting proxy online
//	if status == PROXY_STATE_ONLINE {
//		slots, err := Slots(zkConn, productName)
//		if err != nil {
//			return errors.Trace(err)
//		}
//		for _, slot := range slots {
//			if slot.State.Status != SLOT_STATUS_ONLINE {
//				return errors.Errorf("slot %v is not online", slot)
//			}
//			if slot.GroupId == INVALID_ID {
//				return errors.Errorf("slot %v has invalid group id", slot)
//			}
//		}
//	}
//
//	p.State = status
//	b, _ := json.Marshal(p)
//
//	_, err = zkConn.Set(path.Join(GetProxyPath(productName), proxyName), b, -1)
//	if err != nil {
//		return errors.Trace(err)
//	}
//
//	if status == PROXY_STATE_MARK_OFFLINE {
//		// wait for the proxy down
//		for {
//			_, _, c, err := zkConn.GetW(path.Join(GetProxyPath(productName), proxyName))
//			if zkhelper.ZkErrorEqual(err, zk.ErrNoNode) {
//				return nil
//			} else if err != nil {
//				return errors.Trace(err)
//			}
//			<-c
//			info, err := GetProxyInfo(zkConn, productName, proxyName)
//			log.Info("mark_offline, check proxy status:", proxyName, info, err)
//			if zkhelper.ZkErrorEqual(err, zk.ErrNoNode) {
//				log.Info("shutdown proxy successful")
//				return nil
//			} else if err != nil {
//				return errors.Trace(err)
//			}
//			if info.State == PROXY_STATE_OFFLINE {
//				log.Info("proxy:", proxyName, "offline success!")
//				return nil
//			}
//		}
//	}
//
//	return nil
//}
//
//func GetProxyInfo(zkConn zkhelper.Conn, productName string, proxyName string) (*ProxyInfo, error) {
//	var pi ProxyInfo
//	data, _, err := zkConn.Get(path.Join(GetProxyPath(productName), proxyName))
//	if err != nil {
//		return nil, errors.Trace(err)
//	}
//
//	if err := json.Unmarshal(data, &pi); err != nil {
//		return nil, errors.Trace(err)
//	}
//
//	return &pi, nil
//}
