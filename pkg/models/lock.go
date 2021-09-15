package models

import (
	"fmt"
	"os"
)

type Lock struct {
	Hostname string `json:"hostname"`
	Pid      int    `json:"pid"`
}

func NewLock() *Lock {
	hostname, err := os.Hostname()
	if err != nil {
		panic(err) // should never happen
	}
	pid := os.Getpid()
	return &Lock{Hostname: hostname, Pid: pid}
}

func (t *Lock) Encode() []byte {
	return jsonEncode(t)
}

func (t *Lock) Name() string {
	return fmt.Sprintf("%v-%v", t.Hostname, t.Pid)
}
