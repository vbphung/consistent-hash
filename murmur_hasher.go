package consistenthash

import (
	"github.com/spaolacci/murmur3"
)

func NewMurmurHasher[N Node](replicas int) ConsistentHasher[N] {
	return New[N](replicas, murmurHash)
}

func murmurHash(key string) uint64 {
	return murmur3.Sum64([]byte(key))
}
