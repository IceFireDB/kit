/**
 * @Author: zhangchao
 * @Description:
 * @Date: 2021/9/14 4:27 下午
 */
package client

const (
	EventNodeCreated         = EventType(1)
	EventNodeDeleted         = EventType(2)
	EventNodeDataChanged     = EventType(3)
	EventNodeChildrenChanged = EventType(4)

	EventSession     = EventType(-1)
	EventNotWatching = EventType(-2)
)

var (
	eventNames = map[EventType]string{
		EventNodeCreated:         "EventNodeCreated",
		EventNodeDeleted:         "EventNodeDeleted",
		EventNodeDataChanged:     "EventNodeDataChanged",
		EventNodeChildrenChanged: "EventNodeChildrenChanged",
		EventSession:             "EventSession",
		EventNotWatching:         "EventNotWatching",
	}
)

type EventType int32

func (t EventType) String() string {
	if name := eventNames[t]; name != "" {
		return name
	}
	return "Unknown"
}

type Event struct {
	Type EventType
}

type Client interface {
	Create(path string, data []byte) error
	CreateInOrder(path string, data []byte) (string, error)
	Update(path string, data []byte) error
	Delete(path string) error

	Read(path string, must bool) ([]byte, error)
	//ReadInorderItem(dir string, seq string, must bool) ([]byte, error)
	List(path string, must bool) ([]string, error)

	Close() error

	WatchInOrder(path string) (<-chan Event, []string, error)

	CreateEphemeral(path string, data []byte) (<-chan struct{}, error)
	CreateEphemeralInOrder(path string, data []byte) (<-chan struct{}, string, error)
}