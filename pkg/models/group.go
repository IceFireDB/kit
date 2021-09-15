package models

import (
	"encoding/json"
	"github.com/juju/errors"
	"github.com/ngaut/zkhelper"
)

type ServerType string

const (
	ServerTypeLeader    ServerType = "leader"
	ServerTypeFollower  ServerType = "follower"
	ServerTypeCandidate ServerType = "candidate"
	ServerTypeOffline   ServerType = "offline"
)

type ServerInfo struct {
	Addr string     `json:"addr"`
	Type ServerType `json:"type"`
}

type Server struct {
	ID      int        `json:"id"`
	GroupId int        `json:"group_id"`
	Addr    string     `json:"addr"`
	Type    ServerType `json:"type"` // todo remove
}

type ServerGroup struct {
	Id          int      `json:"id"`
	ProductName string   `json:"product_name"`
	Servers     []Server `json:"servers"`
}

func (g *ServerGroup) Encode() []byte {
	return jsonEncode(g)
}

func (s *Server) Encode() []byte {
	return jsonEncode(s)
}

func (s Server) String() string {
	b, _ := json.MarshalIndent(s, "", "  ")
	return string(b)
}

func (self ServerGroup) String() string {
	b, _ := json.MarshalIndent(self, "", "  ")
	return string(b) + "\n"
}

func GetServer(zkConn zkhelper.Conn, zkPath string) (*Server, error) {
	data, _, err := zkConn.Get(zkPath)
	if err != nil {
		return nil, errors.Trace(err)
	}
	srv := Server{}
	if err := json.Unmarshal(data, &srv); err != nil {
		return nil, errors.Trace(err)
	}
	return &srv, nil
}

func NewServer(serverType ServerType, addr string) *Server {
	return &Server{
		Type:    serverType,
		GroupId: INVALID_ID,
		Addr:    addr,
	}
}

func NewServerGroup(productName string, id int) *ServerGroup {
	return &ServerGroup{
		Id:          id,
		ProductName: productName,
	}
}

func (self *ServerGroup) ServerExists(addr string) (bool, error) {
	if len(self.Servers) == 0 {
		return false, nil
	}
	for _, server := range self.Servers {
		if server.Addr == addr {
			return true, nil
		}
	}
	return false, nil
}
