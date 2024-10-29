package consistenthash

import (
	"sort"
	"strconv"
	"sync"
)

const (
	topWeight   = 100
	minReplicas = 100
)

type HashFunc func(string) uint64

type ConsistentHasher[N Node] interface {
	Add(node N)
	AddWithReplicas(node N, replicas int)
	AddWithWeight(node N, weight int)
	Get(key string) []N
	Remove(node N)
}

type consistentHasher[N Node] struct {
	hash     HashFunc
	replicas int
	vIDs     []uint64
	ring     map[uint64][]N
	nodes    map[string]bool
	mx       sync.RWMutex
}

func New[N Node](replicas int, hash HashFunc) ConsistentHasher[N] {
	return &consistentHasher[N]{
		hash:     hash,
		replicas: max(minReplicas, replicas),
		ring:     make(map[uint64][]N),
		nodes:    make(map[string]bool),
	}
}

func (h *consistentHasher[N]) Add(node N) {
	h.AddWithReplicas(node, h.replicas)
}

func (h *consistentHasher[N]) AddWithReplicas(node N, replicas int) {
	h.Remove(node)

	replicas = min(replicas, h.replicas)
	nodeID := node.ID()

	h.mx.Lock()
	defer h.mx.Unlock()

	h.nodes[nodeID] = true

	for i := range replicas {
		vID := h.hash(nodeID + strconv.Itoa(i))
		h.vIDs = append(h.vIDs, vID)
		h.ring[vID] = append(h.ring[vID], node)
	}

	sort.Slice(h.vIDs, func(i, j int) bool {
		return h.vIDs[i] < h.vIDs[j]
	})
}

func (h *consistentHasher[N]) AddWithWeight(node N, weight int) {
	replicas := h.replicas * weight / topWeight
	h.AddWithReplicas(node, replicas)
}

func (h *consistentHasher[N]) Get(key string) []N {
	h.mx.RLock()
	defer h.mx.RUnlock()

	if len(h.ring) <= 0 {
		return nil
	}

	k := h.hash(key)
	idx := sort.Search(len(h.vIDs), func(i int) bool {
		return h.vIDs[i] >= k
	}) % len(h.vIDs)

	return h.ring[h.vIDs[idx]]
}

func (h *consistentHasher[N]) Remove(node N) {
	nodeID := node.ID()

	h.mx.Lock()
	defer h.mx.Unlock()

	if _, ok := h.nodes[nodeID]; !ok {
		return
	}

	for i := range h.replicas {
		vID := h.hash(nodeID + strconv.Itoa(i))

		idx := sort.Search(len(h.vIDs), func(i int) bool {
			return h.vIDs[i] >= vID
		})
		if idx < len(h.vIDs) && h.vIDs[idx] == vID {
			h.vIDs = append(h.vIDs[:idx], h.vIDs[idx+1:]...)
		}

		nodes, ok := h.ring[vID]
		if !ok {
			continue
		}

		var news []N
		for _, n := range nodes {
			if n.ID() != nodeID {
				news = append(news, n)
			}
		}

		if len(news) > 0 {
			h.ring[vID] = news
		} else {
			delete(h.ring, vID)
		}
	}

	delete(h.nodes, nodeID)
}
