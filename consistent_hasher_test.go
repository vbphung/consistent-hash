package consistenthash

import (
	cryptoRand "crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	n = 32
	m = 10000000
)

type node struct {
	nodeID string
}

var _ Node = (*node)(nil)

func (n *node) ID() string {
	return n.nodeID
}

func TestHasher(t *testing.T) {
	h := NewMurmurHasher[*node](100, 100)

	var (
		nodes []*node
		keys  []string
	)

	for range n {
		nd := &node{genID(t)}
		h.Add(nd)
		nodes = append(nodes, nd)
	}

	for range m {
		keys = append(keys, genID(t))
	}

	fmt.Println(count(t, h, &keys))

	for len(nodes) > 1 {
		i := rand.IntN(len(nodes))
		h.Remove(nodes[i])
		nodes = append(nodes[:i], nodes[i+1:]...)

		nds, cnt := count(t, h, &keys)
		fmt.Println(i, nds, cnt)
	}
}

func count(t *testing.T, h ConsistentHasher[*node], keys *[]string) (string, int) {
	mp := make(map[string]int)
	for _, k := range *keys {
		if n, ok := h.Get(k); ok {
			mp[n.ID()]++
		}
	}

	return display(t, mp)
}

func display(t *testing.T, nds map[string]int) (string, int) {
	cnt := 0
	for _, v := range nds {
		cnt += v
	}
	require.Equal(t, m, cnt)

	buf, err := json.Marshal(slices.Sorted(maps.Values(nds)))
	require.NoError(t, err)

	return string(buf), cnt
}

func genID(t *testing.T) string {
	bytes := make([]byte, 32)

	_, err := cryptoRand.Read(bytes)
	require.NoError(t, err)

	return base64.RawURLEncoding.EncodeToString(bytes)
}
