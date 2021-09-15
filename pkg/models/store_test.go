package models

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
)

const productName = "productNameForTest"

func getStore() *Store {
	//client, err := NewClient("etcd", "localhost:2379", "", time.Second*5)
	client, err := NewClient("zookeeper", "localhost:2181", "", time.Second*5)
	if err != nil {
		panic(err)
	}
	store := NewStore(client, productName)
	return store
}

func TestSlotSetGet(t *testing.T) {
	slot := &Slot{
		ProductName: productName,
		Id:          1000,
		GroupId:     1,
		State:       SlotState{
			Status:        SLOT_STATUS_ONLINE,
			MigrateStatus: SlotMigrateStatus{
				From: INVALID_ID,
				To:   INVALID_ID,
			},
			LastOpTs:      "0",
		},
	}
	s := getStore()
	err := s.UpdateSlot(slot)
	assert.Nil(t, err)
	err = s.DeleteSlot(slot.Id)
	assert.Nil(t, err)
}

func TestMain(m *testing.M) {
	// todo clear when start test
	//s := getStore()
	//err := s.DeletePath(ProductDir(productName))
	//if err != nil {
	//	panic(err)
	//}
	m.Run()
}

func TestAction(t *testing.T) {
	s := getStore()
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		for i := 0; i < 3; i++ {
			c, content, err := s.client.WatchInOrder(GetWatchActionDir(s.product))
			if err != nil {
				t.Fatal(err)
			}
			go func() {
				action := &Action{
					Type:   ACTION_TYPE_MULTI_SLOT_CHANGED,
					Desc:   "desc",
				}
				_, err := s.CreateActoinInOrderer(action)
				if err != nil {
					panic(err)
				}
			}()
			select {
			case <-c:
				fmt.Println("receive watch signal, nodes: ", content)
			case <-time.After(time.Second * 600):
				panic("receive watch signal timeout")
			}
			wg.Done()
		}
	}()
	wg.Wait()
}

func TestGetGroup(t *testing.T) {
	s := getStore()

	t.Run("create action in order", func(t *testing.T) {
		action := &Action{
			Type:   ACTION_TYPE_MULTI_SLOT_CHANGED,
			Desc:   "desc",
			Target: "target",
			Ts:     "ts",
		}
		path, err := s.CreateActoinInOrderer(action)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Println("create action in path", path)
	})
	t.Run("load group not exist", func(t *testing.T) {
		b, err := s.LoadGroup(10000, false)
		assert.Nil(t, err)
		assert.Nil(t, b)
		b, err = s.LoadGroup(10000, true)
		assert.NotEmpty(t, err)
		assert.Nil(t, b)
	})
	t.Run("delete empty", func(t *testing.T) {
		err := s.DeleteGroup(1)
		if err != nil {
			t.Fatal(err)
		}
	})
	t.Run("create group", func(t *testing.T) {
		sg := &ServerGroup{
			Id:          1,
			ProductName: productName,
		}
		err := s.UpdateGroup(sg)
		if err != nil {
			t.Fatal(err)
		}
	})
	t.Run("load dir", func(t *testing.T) {
		fmt.Println(s.client.Read(BaseDir, false))
	})
}