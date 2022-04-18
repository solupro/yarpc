package xclient

import (
	"errors"
	"math"
	"math/rand"
	"sync"
	"time"
)

type SelectMode int

const (
	RANDOM_SELECT SelectMode = iota
	ROUND_ROBIN_SELECT
)

type Discovery interface {
	Refresh() error
	Update(servers []string) error
	Get(mode SelectMode) (string, error)
	GetAll() ([]string, error)
}

type MultiServersDiscovery struct {
	r       *rand.Rand // generate random number
	mu      sync.RWMutex
	servers []string
	index   int // record the selected position for robin algorithm
}

func NewMultiServersDiscovery(servers []string) *MultiServersDiscovery {
	discovery := &MultiServersDiscovery{
		servers: servers,
		r:       rand.New(rand.NewSource(time.Now().UnixNano())),
	}

	discovery.index = discovery.r.Intn(math.MaxInt32 - 1)
	return discovery
}

var _ Discovery = (*MultiServersDiscovery)(nil)

func (m *MultiServersDiscovery) Refresh() error {
	return nil
}

func (m *MultiServersDiscovery) Update(servers []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.servers = servers
	return nil
}

func (m *MultiServersDiscovery) Get(mode SelectMode) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := len(m.servers)
	if 0 == n {
		return "", errors.New("rpc discovery: no available servers")
	}

	switch mode {
	case RANDOM_SELECT:
		return m.servers[m.r.Intn(n)], nil
	case ROUND_ROBIN_SELECT:
		s := m.servers[m.index%n]
		m.index = (m.index + 1) % n
		return s, nil
	default:
		return "", errors.New("rpc discovery: not supported select mode")
	}
}

func (m *MultiServersDiscovery) GetAll() ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	n := len(m.servers)
	servers := make([]string, n, n)
	copy(servers, m.servers)
	return servers, nil
}
