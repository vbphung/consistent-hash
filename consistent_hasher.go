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
	mu          sync.RWMutex
}

func New[N Node](maxVirNodes, maxWeight int, hash HashFunc) ConsistentHasher[N] {
	return &consistentHasher[N]{
		hash:        hash,
		maxVirNodes: maxVirNodes,
		maxWeight:   maxWeight,
		ring:        make(map[uint64]N),
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

	for i := range h.maxVirNodes {
		vID := h.hash(nodeID + strconv.Itoa(i))
		if ok := h.findAndRemove(vID); !ok {
			break
		}
	}
}

func (h *consistentHasher[N]) findAndRemove(vID uint64) bool {
	idx := sort.Search(len(h.vIDs), func(i int) bool {
		return h.vIDs[i] >= vID
	})
	if idx < 0 || idx >= len(h.vIDs) || h.vIDs[idx] != vID {
		return false
	}

	h.vIDs = sliceRemoveAt(h.vIDs, idx)
	delete(h.ring, vID)

	return true
}

func (h *consistentHasher[N]) zero() (n N) {
	return
}

func sliceRemoveAt[T any](s []T, i int) []T {
	return append(s[:i], s[i+1:]...)
}
