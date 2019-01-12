package frontend

import (
	"time"
    "sync"

	rclient "router/client"
	"router/router"
	"storage"
)

// InitTimeout is a timeout to wait after unsuccessful List() request to Router.
//
// InitTimeout -- количество времени, которое нужно подождать до следующей попытки
// отправки запроса List() в Router.
const InitTimeout = 100 * time.Millisecond

// Config stores configuration for a Frontend service.
//
// Config -- содержит конфигурацию Frontend.
type Config struct {
	// Addr is an address to listen at.
	// Addr -- слушающий адрес Frontend.
	Addr storage.ServiceAddr
	// Router is an address of Router service.
	// Router -- адрес Router service.
	Router storage.ServiceAddr

	// NC specifies client for Node.
	// NC -- клиент для node.
	NC storage.Client `yaml:"-"`
	// RC specifies client for Router.
	// RC -- клиент для router.
	RC rclient.Client `yaml:"-"`
	// NodesFinder specifies a NodeFinder to use.
	// NodesFinder -- NodesFinder, который нужно использовать в Frontend.
	NF router.NodesFinder `yaml:"-"`
}

// Frontend is a frontend service.
type Frontend struct {
	// TODO: implement
    config Config
	nodes []storage.ServiceAddr
	once sync.Once
}

// New creates a new Frontend with a given cfg.
//
// New создает новый Frontend с данным cfg.
func New(cfg Config) *Frontend {
	// TODO: implement
    return &Frontend{config: cfg, nodes: nil}
}

// Put an item to the storage if an item for the given key doesn't exist.
// Returns error otherwise.
//
// Put -- добавить запись в хранилище, если запись для данного ключа
// не существует. Иначе вернуть ошибку.

func (fe *Frontend) Put(k storage.RecordID, d []byte) error {
	return fe.putDel(k, func(node storage.ServiceAddr) error {
		return fe.config.NC.Put(node, k, d)
	})
}

func (fe *Frontend) putDel(id storage.RecordID, action func(node storage.ServiceAddr) error) error {
	// TODO: implement
    nodes, err := fe.config.RC.NodesFind(fe.config.Router, id)
	if err != nil {
		return err
	}

	if len(nodes) < storage.MinRedundancy {
		return storage.ErrNotEnoughDaemons
	}

	errors := make(chan error, len(nodes))
	for _, node := range nodes {
		go func(node storage.ServiceAddr) {
			errors <- action(node)
		}(node)
	}

	errorMap := make(map[error]int)
	for range nodes {
		err := <-errors
		if err != nil {
			errorMap[err]++
		}
	}

	for err, ind := range errorMap {
		if ind >= storage.MinRedundancy {
			return err
		}
	}

	count := len(nodes) - len(errorMap)
	if count >= storage.MinRedundancy {
		return nil
	}

	return storage.ErrQuorumNotReached
}

// Del an item from the storage if an item exists for the given key.
// Returns error otherwise.
//
// Del -- удалить запись из хранилища, если запись для данного ключа
// существует. Иначе вернуть ошибку.
func (fe *Frontend) Del(k storage.RecordID) error {
	// TODO: implement
    return fe.putDel(k, func(node storage.ServiceAddr) error {
		return fe.config.NC.Del(node, k)
	})
}

// Get an item from the storage if an item exists for the given key.
// Returns error otherwise.
//
// Get -- получить запись из хранилища, если запись для данного ключа
// существует. Иначе вернуть ошибку.
func (fe *Frontend) Get(k storage.RecordID) ([]byte, error) {
	// TODO: implement
    fe.once.Do(func() {
		for {
			var err error
			fe.nodes, err = fe.config.RC.List(fe.config.Router)
			if err == nil {
				break
			}
			time.Sleep(InitTimeout)
		}
	})

	nodes := fe.config.NF.NodesFind(k, fe.nodes)
	if len(nodes) < storage.MinRedundancy {
		return nil, storage.ErrNotEnoughDaemons
	}

	type res struct {
		data []byte
		err  error
	}

	dataChan := make(chan res, len(nodes))
	for _, node := range nodes {
		go func(node storage.ServiceAddr) {
			data, err := fe.config.NC.Get(node, k)
			dataChan <- res{data, err}
		}(node)
	}

	recordsCnt := make(map[string]int)
	errorsCnt := make(map[error]int)
	for range nodes {
		result := <-dataChan
		if result.err == nil {
			recordsCnt[string(result.data)]++
			if recordsCnt[string(result.data)] >= storage.MinRedundancy {
				return result.data, nil
			}
			continue
		}

		errorsCnt[result.err]++
		if errorsCnt[result.err] >= storage.MinRedundancy {
			return nil, result.err
		}
	}
	return nil, storage.ErrQuorumNotReached
}
