package consistenthash

import (
	"sort"
	"strconv"
	"sync"
)

type HashFunc func(string) uint64

type ConsistentHasher[N Node] interface {
	Add(node N)
	AddByVirtualNodes(node N, virNodes int)
	AddWithWeight(node N, weight int)
	Get(key string) (N, bool)
	Remove(node N)
}

type consistentHasher[N Node] struct {
	hash        HashFunc
	maxVirNodes int
	maxWeight   int
	vIDs        []uint64
	ring        map[uint64]N
	nodes       map[string]bool
	mu          sync.RWMutex
}

func New[N Node](maxVirNodes, maxWeight int, hash HashFunc) ConsistentHasher[N] {
	return &consistentHasher[N]{
		hash:        hash,
		maxVirNodes: maxVirNodes,
		maxWeight:   maxWeight,
		ring:        make(map[uint64]N),
		nodes:       make(map[string]bool),
	}
}

func (h *consistentHasher[N]) Add(node N) {
	h.AddByVirtualNodes(node, h.maxVirNodes)
}

func (h *consistentHasher[N]) AddByVirtualNodes(node N, virNodes int) {
	h.Remove(node)

	virNodes = min(virNodes, h.maxVirNodes)
	nodeID := node.ID()

	h.mu.Lock()
	defer h.mu.Unlock()

	h.nodes[nodeID] = true

	for i := range virNodes {
		vID := h.hash(nodeID + strconv.Itoa(i))
		h.vIDs = append(h.vIDs, vID)
		h.ring[vID] = node
	}

	sort.Slice(h.vIDs, func(i, j int) bool {
		return h.vIDs[i] < h.vIDs[j]
	})
}

func (h *consistentHasher[N]) AddWithWeight(node N, weight int) {
	virNodes := h.maxVirNodes * weight / h.maxWeight
	h.AddByVirtualNodes(node, virNodes)
}

func (h *consistentHasher[N]) Get(key string) (N, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if len(h.ring) <= 0 {
		return h.zero(), false
	}

	k := h.hash(key)
	idx := sort.Search(len(h.vIDs), func(i int) bool {
		return h.vIDs[i] >= k
	}) % len(h.vIDs)

	n, ok := h.ring[h.vIDs[idx]]
	return n, ok
}

func (h *consistentHasher[N]) Remove(node N) {
	nodeID := node.ID()

	h.mu.Lock()
	defer h.mu.Unlock()

	if _, ok := h.nodes[nodeID]; !ok {
		return
	}

	for i := range h.maxVirNodes {
		vID := h.hash(nodeID + strconv.Itoa(i))

		idx := sort.Search(len(h.vIDs), func(i int) bool {
			return h.vIDs[i] >= vID
		})
		if idx < len(h.vIDs) && h.vIDs[idx] == vID {
			h.vIDs = append(h.vIDs[:idx], h.vIDs[idx+1:]...)
		}

		delete(h.ring, vID)
	}

	delete(h.nodes, nodeID)
}

func (h *consistentHasher[N]) zero() (n N) {
	return
}
