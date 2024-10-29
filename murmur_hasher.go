package consistenthash

import (
	"github.com/spaolacci/murmur3"
)

func NewMurmurHasher[N Node](maxVirNodes, maxWeight int) ConsistentHasher[N] {
	return New[N](maxVirNodes, maxWeight, murmurHash)
}

func murmurHash(key string) uint64 {
	return murmur3.Sum64([]byte(key))
}
