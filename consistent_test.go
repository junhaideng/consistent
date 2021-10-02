package consistent

import (
	"fmt"
	"math/rand"
	"testing"
)

func TestConsistentHash(t *testing.T) {
	c := New(WithReplicas(20))
	ips := []string{"192.168.0.1", "192.168.0.2", "192.168.0.3", "192.168.0.4"}

	for _, ip := range ips {
		c.Add(ip)
	}
	statistic := make(map[string]int)
	for i := 0; i < 10000; i++ {

		key := fmt.Sprintf("%d-%d", rand.Intn(i+1), rand.Intn(i+1))
		statistic[c.Get(key)]++
	}

	t.Log(statistic)
}

func BenchmarkConsistentHash(b *testing.B) {
	c := New()

	b.Run("Add", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			c.Add(fmt.Sprintf("nodes-%d", i))
		}
	})

	b.Run("Get", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			c.Get(fmt.Sprintf("key-%d", i))
		}
	})
	
	b.Run("Delete", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			c.Delete(fmt.Sprintf("nodes-%d", i))
		}
	})
}
