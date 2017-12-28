// 2017/12/27 16:16:14 Wed
package pool_test

import (
	"net"
	"testing"
	"time"

	"github.com/hms58/pool"
)

func dummyDialer() (interface{}, error) {
	return &net.TCPConn{}, nil
}

func benchmarkPoolGetPut(b *testing.B, poolSize int) {
	close := func(v interface{}) error { return v.(net.Conn).Close() }
	connPool := pool.NewChannelPool(&pool.PoolConfig{
		// InitialCap: 1,
		MaxCap:  poolSize,
		Factory: dummyDialer,
		Close:   close,
		//链接最大空闲时间，超过该时间的链接 将会关闭，可避免空闲时链接EOF，自动失效的问题
		IdleTimeout: 15 * time.Second,
	})

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			cn, err := connPool.Get()
			if err != nil {
				b.Fatal(err)
			}
			if err = connPool.Put(cn); err != nil {
				b.Fatal(err)
			}
		}
	})
	// connPool.ShowStats()
}

func BenchmarkPoolGetPut10Conns(b *testing.B) {
	benchmarkPoolGetPut(b, 10)
}

func BenchmarkPoolGetPut100Conns(b *testing.B) {
	benchmarkPoolGetPut(b, 100)
}

func BenchmarkPoolGetPut1000Conns(b *testing.B) {
	benchmarkPoolGetPut(b, 1000)
}

func benchmarkPoolGetRemove(b *testing.B, poolSize int) {
	// close := func(v interface{}) error { return v.(net.Conn).Close() }
	connPool := pool.NewChannelPool(&pool.PoolConfig{
		// InitialCap: 1,
		MaxCap:  poolSize,
		Factory: dummyDialer,
		// 不需要关闭
		Close: nil,
		//链接最大空闲时间，超过该时间的链接 将会关闭，可避免空闲时链接EOF，自动失效的问题
		IdleTimeout: 15 * time.Second,
	})

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			cn, err := connPool.Get()
			if err != nil {
				b.Fatal(err)
			}
			if err := connPool.Close(cn); err != nil {
				b.Fatal(err)
			}
		}
	})
	// connPool.ShowStats()
}

func BenchmarkPoolGetRemove10Conns(b *testing.B) {
	benchmarkPoolGetRemove(b, 10)
}

func BenchmarkPoolGetRemove100Conns(b *testing.B) {
	benchmarkPoolGetRemove(b, 100)
}

func BenchmarkPoolGetRemove1000Conns(b *testing.B) {
	benchmarkPoolGetRemove(b, 1000)
}
