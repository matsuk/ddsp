package node

import (
	"time"
    "sync"

	router "router/client"
	"storage"
)

// Config stores configuration for a Node service.
//
// Config -- содержит конфигурацию Node.
type Config struct {
	// Addr is an address to listen at.
	// Addr -- слушающий адрес Node.
	Addr storage.ServiceAddr
	// Router is an address of Router service.
	// Router -- адрес Router service.
	Router storage.ServiceAddr
	// Hearbeat is a time interval between hearbeats.
	// Hearbeat -- интервал между двумя heartbeats.
	Heartbeat time.Duration

	// Client specifies client for Router.
	// Client -- клиент для Router.
	Client router.Client `yaml:"-"`
}

// Node is a Node service.
type Node struct {
	// TODO: implement
    config Config
	data map[storage.RecordID][]byte
    hb chan bool
	sync.Mutex
}

// New creates a new Node with a given cfg.
//
// New создает новый Node с данным cfg.
func New(cfg Config) *Node {
	// TODO: implement
    return &Node{config: cfg, data: make(map[storage.RecordID][]byte), hb: make(chan bool)}
}

// Hearbeats runs heartbeats from node to a router
// each time interval set by cfg.Hearbeat.
//
// Hearbeats запускает отправку heartbeats от node к router
// через каждый интервал времени, заданный в cfg.Heartbeat.
func (node *Node) Heartbeats() {
	// TODO: implement
    go func() {
	    for {
		    select {
		    case <- node.hb:
			    return
		    default:
			    time.Sleep(node.config.Heartbeat)
			    node.config.Client.Heartbeat(node.config.Router, node.config.Addr)
			}
		}
	}()
}

// Stop stops heartbeats
//
// Stop останавливает отправку heartbeats.
func (node *Node) Stop() {
	// TODO: implement
    node.hb <- true
}

// Put an item to the node if an item for the given key doesn't exist.
// Returns the storage.ErrRecordExists error otherwise.
//
// Put -- добавить запись в node, если запись для данного ключа
// не существует. Иначе вернуть ошибку storage.ErrRecordExists.
func (node *Node) Put(k storage.RecordID, d []byte) error {
	// TODO: implement
    node.Lock()
	defer node.Unlock()
	if _, err := node.data[k]; err {
		return storage.ErrRecordExists
	}
	node.data[k] = d
	return nil
}

// Del an item from the node if an item exists for the given key.
// Returns the storage.ErrRecordNotFound error otherwise.
//
// Del -- удалить запись из node, если запись для данного ключа
// существует. Иначе вернуть ошибку storage.ErrRecordNotFound.
func (node *Node) Del(k storage.RecordID) error {
	// TODO: implement
    node.Lock()
	defer node.Unlock()
	if _, err := node.data[k]; !err {
        return storage.ErrRecordNotFound
	}
	delete(node.data, k)
    return nil
}

// Get an item from the node if an item exists for the given key.
// Returns the storage.ErrRecordNotFound error otherwise.
//
// Get -- получить запись из node, если запись для данного ключа
// существует. Иначе вернуть ошибку storage.ErrRecordNotFound.
func (node *Node) Get(k storage.RecordID) ([]byte, error) {
	// TODO: implement
    node.Lock()
	defer node.Unlock()
	if d, err := node.data[k]; err {
		return d, nil
	}
	return nil, storage.ErrRecordNotFound
}
