package pool

import (
	"errors"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

//PoolConfig 连接池相关配置
type PoolConfig struct {
	// 连接池中拥有的最小连接数
	// InitialCap int
	//连接池中拥有的最大的连接数
	MaxCap int
	//生成连接的方法
	Factory func() (interface{}, error)
	//关闭链接的方法
	Close func(interface{}) error
	//链接最大空闲时间，超过该事件则将失效
	IdleTimeout time.Duration
}

//channelPool 存放链接信息
type channelPool struct {
	mu          sync.Mutex
	conns       chan *idleConn
	factory     func() (interface{}, error)
	close       func(interface{}) error
	idleTimeout time.Duration

	busyConnsMu sync.Mutex
	busyConns   []*idleConn

	stats Stats
}

type idleConn struct {
	conn interface{}
	t    time.Time
}

type Stats struct {
	Hits   uint32 // number of times free connection was found in the pool
	Misses uint32 // number of times free connection was NOT found in the pool

	TotalConns uint32 // number of total connections in the pool
}

var _ Pooler = (*channelPool)(nil)

// NewChannelPool 初始化链接
func NewChannelPool(poolConfig *PoolConfig) Pooler {
	if poolConfig.MaxCap <= 0 {
		poolConfig.MaxCap = 10
	}

	c := &channelPool{
		conns:       make(chan *idleConn, poolConfig.MaxCap),
		busyConns:   make([]*idleConn, 0, poolConfig.MaxCap),
		factory:     poolConfig.Factory,
		close:       poolConfig.Close,
		idleTimeout: poolConfig.IdleTimeout,
	}

	// for i := 0; i < poolConfig.InitialCap; i++ {
	// 	conn, err := c.factory()
	// 	if err != nil {
	// 		c.Release()
	// 		return nil, fmt.Errorf("factory is not able to fill the pool: %s", err)
	// 	}
	// 	c.conns <- &idleConn{conn: conn, t: time.Now()}
	// }

	return c
}

//getConns 获取所有连接
func (c *channelPool) getConns() chan *idleConn {
	c.mu.Lock()
	conns := c.conns
	c.mu.Unlock()
	return conns
}

// Get 从pool中取一个连接
func (c *channelPool) Get() (interface{}, error) {
	conns := c.getConns()
	if conns == nil {
		return nil, ErrClosed
	}
	for {
		select {
		case wrapConn := <-conns:
			if wrapConn == nil {
				continue
				// return nil, ErrClosed
			}
			// 判断是否超时，超时则丢弃
			if timeout := c.idleTimeout; timeout > 0 {
				if wrapConn.t.Add(timeout).Before(time.Now()) {
					// 丢弃并关闭该链接
					c.Close(wrapConn.conn)
					continue
				}
			}

			// c.pushBusy(wrapConn)
			atomic.AddUint32(&c.stats.Hits, 1)
			return wrapConn.conn, nil
		default:
			conn, err := c.factory()
			if err != nil {
				return nil, err
			}
			// c.pushBusy(&idleConn{conn: conn, t: time.Now()})
			atomic.AddUint32(&c.stats.Misses, 1)
			return conn, nil
		}
	}
}

// Put 将连接放回pool中
func (c *channelPool) Put(conn interface{}) error {
	if conn == nil {
		return errors.New("pool is nil. rejecting")
	}

	c.mu.Lock()
	// defer c.mu.Unlock()

	if c.conns == nil {
		c.mu.Unlock()
		return c.Close(conn)
	}
	c.mu.Unlock()

	// cn := c.popBusy()
	// if cn != nil {
	// 	cn.conn = conn
	// 	cn.t = time.Now()
	// }

	select {
	case c.conns <- &idleConn{conn: conn, t: time.Now()}:
		// case c.conns <- cn:
		return nil
	default:
		// 连接池已满，直接关闭该链接
		return c.Close(conn)
	}
}

//Close 关闭单条连接
func (c *channelPool) Close(conn interface{}) error {
	if conn == nil {
		return errors.New("pool is nil. rejecting")
	}
	if c.close != nil {
		return c.close(conn)
	}
	return nil
}

//Release 释放连接池中所有链接
func (c *channelPool) Release() {
	c.mu.Lock()
	conns := c.conns
	c.conns = nil
	c.factory = nil
	closeFun := c.close
	c.close = nil
	c.mu.Unlock()

	if conns == nil {
		return
	}

	close(conns)
	for wrapConn := range conns {
		closeFun(wrapConn.conn)
	}
}

//Len 连接池中已有的连接
func (c *channelPool) Len() int {
	return len(c.getConns())
}

func (p *channelPool) popBusy() *idleConn {
	p.busyConnsMu.Lock()
	defer p.busyConnsMu.Unlock()

	if len(p.busyConns) == 0 {
		return nil
	}

	idx := len(p.busyConns) - 1
	cn := p.busyConns[idx]
	p.busyConns = p.busyConns[:idx]
	return cn
}

func (p *channelPool) pushBusy(cn *idleConn) {
	if cn != nil {
		p.busyConnsMu.Lock()
		defer p.busyConnsMu.Unlock()

		p.busyConns = append(p.busyConns, cn)
	}
}

func (p *channelPool) BusyLen() int {
	p.busyConnsMu.Lock()
	defer p.busyConnsMu.Unlock()
	return len(p.busyConns)
}

func (p *channelPool) Stats() *Stats {
	return &Stats{
		Hits:       atomic.LoadUint32(&p.stats.Hits),
		Misses:     atomic.LoadUint32(&p.stats.Misses),
		TotalConns: uint32(p.Len()),
	}
}

func (p *channelPool) ShowStats() {
	stats := p.Stats()
	log.Printf("TotalConns: %d", stats.TotalConns)
	log.Printf("Hits: %d	Misses: %d", stats.Hits, stats.Misses)
}
