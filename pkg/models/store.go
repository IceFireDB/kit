// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.
//

package models

import (
	"encoding/json"
	"fmt"
	"log"
	"path/filepath"
	"regexp"

	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/IceFireDB/kit/pkg/models/client"
)

func init() {
	if filepath.Separator != '/' {
		log.Panicf("bad Separator = '%c', must be '/'", filepath.Separator)
	}
}

const BaseDir = "/icefire"

var ErrGroupMasterNotFound = errors.New("group master not found")

func ProductDir(product string) string {
	return filepath.Join(BaseDir, product)
}

func GetWatchActionDir(product string) string {
	return filepath.Join(BaseDir, product, "actions")
}

func ActionPath(product string, seq string) string {
	return filepath.Join(BaseDir, product, "actions", seq)
}

func LockPath(product string) string {
	return filepath.Join(BaseDir, product, "pd")
}

func CliDir(product string) string {
	return filepath.Join(BaseDir, product, "living-cli-config")
}

func CliPath(product string, name string) string {
	return filepath.Join(BaseDir, product, "living-cli-config", name)
}

func ProxyDir(product string) string {
	return filepath.Join(BaseDir, product, "proxy")
}

func ProxyPath(product string, id string) string {
	return filepath.Join(BaseDir, product, "proxy", id)
}

func SlotDir(product string) string {
	return filepath.Join(BaseDir, product, "slots")
}

func SlotPath(product string, sid int) string {
	return filepath.Join(BaseDir, product, "slots", fmt.Sprintf("slot-%04d", sid))
}

func GroupDir(product string) string {
	return filepath.Join(BaseDir, product, "group")
}

func ServerDir(product string) string {
	return filepath.Join(BaseDir, product, "server")
}

func GroupPath(product string, gid int) string {
	return filepath.Join(BaseDir, product, "group", fmt.Sprintf("group-%04d", gid))
}

func ServerPath(product string, addr string) string {
	return filepath.Join(BaseDir, product, "server", fmt.Sprintf("server-%s", addr))
}

func LoadTopom(client client.Client, product string, must bool) (*Topom, error) {
	b, err := client.Read(LockPath(product), must)
	if err != nil || b == nil {
		return nil, err
	}
	t := &Topom{}
	if err := jsonDecode(t, b); err != nil {
		return nil, err
	}
	return t, nil
}

type Store struct {
	client  client.Client
	product string
}

func NewStore(client client.Client, product string) *Store {
	return &Store{client, product}
}

func (s *Store) Close() error {
	return s.client.Close()
}

func (s *Store) Client() client.Client {
	return s.client
}

func (s *Store) LockPath() string {
	return LockPath(s.product)
}

func (s *Store) SlotDir() string {
	return SlotDir(s.product)
}

func (s *Store) SlotPath(sid int) string {
	return SlotPath(s.product, sid)
}

func (s *Store) ProxyDir() string {
	return ProxyDir(s.product)
}

func (s *Store) ProxyPath(id string) string {
	return ProxyPath(s.product, id)
}

func (s *Store) CliDir() string {
	return CliDir(s.product)
}

func (s *Store) CliPath(name string) string {
	return CliPath(s.product, name)
}

func (s *Store) GroupDir() string {
	return GroupDir(s.product)
}

func (s *Store) GroupPath(gid int) string {
	return GroupPath(s.product, gid)
}

func (s *Store) ServerDir() string {
	return ServerDir(s.product)
}

func (s *Store) ActionPath(seq string) string {
	return ActionPath(s.product, seq)
}

func (s *Store) ServerPath(addr string) string {
	return ServerPath(s.product, addr)
}

func (s *Store) DeletePath(path string) error {
	return s.client.Delete(path)
}

func (s *Store) Lock() error {
	return s.client.Create(s.LockPath(), NewLock().Encode())
}

func (s *Store) UnLock() error {
	return s.client.Delete(s.LockPath())
}

func (s *Store) Acquire(topom *Topom) error {
	return s.client.Create(s.LockPath(), topom.Encode())
}

func (s *Store) Release() error {
	return s.client.Delete(s.LockPath())
}

func (s *Store) LoadTopom(must bool) (*Topom, error) {
	return LoadTopom(s.client, s.product, must)
}

func (s *Store) LoadProxy(id string) (*ProxyInfo, error) {
	data, err := s.client.Read(s.ProxyPath(id), true)
	if err != nil || data == nil {
		return nil, err
	}
	var p ProxyInfo
	if err := json.Unmarshal(data, &p); err != nil {
		return nil, err
	}

	return &p, nil
}

func (s *Store) UpdateProxy(proxyInfo *ProxyInfo) error {
	return s.client.Update(s.ProxyPath(proxyInfo.Id), proxyInfo.Encode())
}

func (s *Store) DeleteProxy(id string) error {
	return s.client.Delete(s.ProxyPath(id))
}

func (s *Store) GetSlot(sid int, must bool) (*Slot, error) {
	data, err := s.client.Read(s.SlotPath(sid), must)
	if err != nil || data == nil {
		return nil, err
	}
	var slot Slot
	if err := json.Unmarshal(data, &slot); err != nil {
		return nil, err
	}

	return &slot, nil
}

func (s *Store) InitSlotSet(productName string, totalSlotNum int) error {
	for i := 0; i < totalSlotNum; i++ {
		slot := NewSlot(productName, i)
		if err := s.UpdateSlot(slot); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) UpdateSlotWithoutAction(m *Slot) error {
	err := s.client.Update(s.SlotPath(m.Id), m.Encode())
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (s *Store) UpdateSlot(m *Slot) error {
	switch m.State.Status {
	case SLOT_STATUS_MIGRATE, SLOT_STATUS_OFFLINE,
		SLOT_STATUS_ONLINE, SLOT_STATUS_PRE_MIGRATE:
		{
			// valid status, OK
		}
	default:
		{
			return errors.Trace(ErrUnknownSlotStatus)
		}
	}
	err := s.client.Update(s.SlotPath(m.Id), m.Encode())
	if err != nil {
		return errors.Trace(err)
	}
	if m.State.Status == SLOT_STATUS_MIGRATE {
		err = s.NewAction(ACTION_TYPE_SLOT_MIGRATE, m, "", true)
	} else {
		err = s.NewAction(ACTION_TYPE_SLOT_CHANGED, m, "", true)
	}
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (s *Store) DeleteSlot(sid int) error {
	return s.client.Delete(s.SlotPath(sid))
}

func (s *Store) ListGroup() (map[int]*ServerGroup, error) {
	paths, err := s.client.List(s.GroupDir(), false)
	if err != nil {
		return nil, err
	}
	group := make(map[int]*ServerGroup)
	for _, path := range paths {
		b, err := s.client.Read(path, true)
		if err != nil {
			return nil, err
		}
		g := &ServerGroup{}
		if err := jsonDecode(g, b); err != nil {
			return nil, err
		}
		group[g.Id] = g
	}
	return group, nil
}

func (s *Store) LoadGroup(gid int, must bool) (*ServerGroup, error) {
	b, err := s.client.Read(s.GroupPath(gid), must)
	if err != nil || b == nil {
		return nil, err
	}
	g := &ServerGroup{}
	if err := jsonDecode(g, b); err != nil {
		return nil, err
	}
	return g, nil
}

func (s *Store) Exists(path string) (bool, error) {
	b, err := s.client.Read(path, false)
	if err != nil {
		return false, err
	}
	return b != nil, nil
}

func (s *Store) GroupExists(gid int) (bool, error) {
	g, err := s.LoadGroup(gid, false)
	if err != nil {
		return false, err
	}
	return g != nil, nil
}

func (s *Store) UpdateGroup(g *ServerGroup) error {
	return s.client.Update(s.GroupPath(g.Id), g.Encode())
}

func (s *Store) DeleteGroup(gid int) error {
	return s.client.Delete(s.GroupPath(gid))
}

func (s *Store) GetServer(addr string, must bool) (*Server, error) {
	data, err := s.client.Read(s.ServerPath(addr), must)
	if err != nil || data == nil {
		return nil, err
	}
	var server Server
	if err := json.Unmarshal(data, &server); err != nil {
		return nil, err
	}

	return &server, nil
}

func (s *Store) GetServerByPath(path string, must bool) (*Server, error) {
	data, err := s.client.Read(path, must)
	if err != nil || data == nil {
		return nil, err
	}
	var server Server
	if err := json.Unmarshal(data, &server); err != nil {
		return nil, err
	}

	return &server, nil
}

func (s *Store) UpdateServer(server *Server) error {
	return s.client.Update(s.ServerPath(server.Addr), server.Encode())
}

func (s *Store) DeleteServer(addr string) error {
	return s.client.Delete(s.ServerPath(addr))
}

func (s *Store) CreateActoinInOrderer(a *Action) (p string, err error) {
	path := GetWatchActionDir(s.product)
	return s.client.CreateInOrder(path, a.Encode())
}

func (s *Store) DeleteAction(id int) error {
	return nil // todo
}

func (s *Store) WatchActions() (<-chan client.Event, []string, error) {
	return s.client.WatchInOrder(GetWatchActionDir(s.product))
}

func ValidateProduct(name string) error {
	if regexp.MustCompile(`^\w[\w\.\-]*$`).MatchString(name) {
		return nil
	}
	return errors.Errorf("bad product name = %s", name)
}

func (s *Store) SetSlotRange(productName string, fromSlot, toSlot, groupId int, status SlotStatus) error {
	if status != SLOT_STATUS_OFFLINE && status != SLOT_STATUS_ONLINE {
		return errors.New("invalid status")
	}

	ok, err := s.LoadGroup(groupId, true)
	if err != nil {
		return err
	}
	if ok == nil {
		return fmt.Errorf("group id %d not exist", groupId)
	}

	for i := fromSlot; i <= toSlot; i++ {
		slot, err := s.GetSlot(i, false)
		if slot == nil {
			slot = &Slot{}
		}
		if err != nil {
			return errors.Trace(err)
		}
		slot.GroupId = groupId
		slot.State.Status = status
		err = s.UpdateSlotWithoutAction(slot)

		if err != nil {
			return errors.Trace(err)
		}
	}

	param := SlotMultiSetParam{
		From:    fromSlot,
		To:      toSlot,
		GroupId: groupId,
		Status:  status,
	}
	err = s.NewAction(ACTION_TYPE_MULTI_SLOT_CHANGED, param, "", true)
	return errors.Trace(err)
}

// todo only need sg id
func (s *Store) GetServers(sg *ServerGroup) ([]Server, error) {
	var ret []Server
	for _, server := range sg.Servers {
		s, err := s.GetServer(server.Addr, true)
		if err != nil {
			return nil, errors.Trace(err)
		}
		ret = append(ret, *s)
	}
	return ret, nil
}

func (s *Store) Master(sg *ServerGroup) (*Server, error) {
	servers, err := s.GetServers(sg)
	if err != nil {
		return nil, errors.Trace(err)
	}
	for _, s := range servers {
		// TODO check if there are two masters
		if s.Type == ServerTypeLeader {
			return &s, nil
		}
	}
	return nil, ErrGroupMasterNotFound
}

func (s *Store) CheckAsOnlyMaster(sg *ServerGroup) (*Server, error) {
	servers, err := s.GetServers(sg)
	if err != nil {
		return nil, errors.Trace(err)
	}
	for _, s := range servers {
		// TODO check if there are two masters
		if s.Type == ServerTypeLeader {
			return &s, nil
		}
	}
	return nil, ErrGroupMasterNotFound
}

// ------------ cli ------------

func (s *Store) RegisterActiveCli(l *Lock) error {
	name := l.Name()
	return s.client.Update(s.CliPath(name), l.Encode())
}

func (s *Store) UnregisterActiveCli(name string) error {
	return s.client.Delete(s.CliPath(name))
}
