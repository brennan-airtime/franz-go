package kfake

import (
	"fmt"
	"net"
	"sync/atomic"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// TODO
//
// * Handle requests concurrently, i.e. JoinGroup
//   * Actually, just spin out concurrent group manager that then hooks back
//     into the control loop
//
// * Add raft and make the brokers independent
//
// * Support multiple replicas -- we just pass this through
// * Support per-partition leader epoch

type (

	// Cluster is a mock Kafka broker cluster.
	Cluster struct {
		cfg cfg

		controller *broker
		bs         []*broker

		reqCh chan clientReq

		epoch int32
		data  data
		pids  pids

		die  chan struct{}
		dead atomic.Bool
	}

	broker struct {
		c    *Cluster
		ln   net.Listener
		node int32
	}
)

// NewCluster returns a new mocked Kafka cluster.
func NewCluster(opts ...Opt) (c *Cluster, err error) {
	cfg := cfg{
		nbrokers:        3,
		logger:          new(nopLogger),
		clusterID:       "kfake",
		defaultNumParts: 10,
	}
	for _, opt := range opts {
		opt.apply(&cfg)
	}

	c = &Cluster{
		cfg: cfg,

		reqCh: make(chan clientReq, 20),

		data: data{
			m:    make(map[string]map[int32]*partData),
			id2t: make(map[uuid]string),
			t2id: make(map[string]uuid),
		},

		die: make(chan struct{}),
	}
	c.data.c = c
	defer func() {
		if err != nil {
			c.Close()
		}
	}()

	for i := 0; i < cfg.nbrokers; i++ {
		b := &broker{
			c:    c,
			ln:   newListener(),
			node: int32(i + 1),
		}
		c.bs = append(c.bs, b)
		go b.listen()
	}
	c.controller = c.bs[len(c.bs)-1]
	go c.run()
	return c, nil
}

// ListenAddrs returns the hostports that the cluster is listening on.
func (c *Cluster) ListenAddrs() []string {
	var addrs []string
	for _, b := range c.bs {
		addrs = append(addrs, b.ln.Addr().String())
	}
	return addrs
}

// Close shuts down the cluster.
func (c *Cluster) Close() {
	if c.dead.Swap(true) {
		return
	}
	close(c.die)
	for _, b := range c.bs {
		b.ln.Close()
	}
}

func newListener() net.Listener {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		if l, err = net.Listen("tcp6", "[::1]:0"); err != nil {
			panic(fmt.Sprintf("kfake: failed to listen on a port: %v", err))
		}
	}
	return l
}

func (b *broker) listen() {
	defer b.ln.Close()
	for {
		conn, err := b.ln.Accept()
		if err != nil {
			return
		}

		cc := &clientConn{
			c:      b.c,
			b:      b,
			conn:   conn,
			respCh: make(chan clientResp, 2),
		}
		go cc.read()
		go cc.write()
	}
}

type clientReq struct {
	cc   *clientConn
	kreq kmsg.Request
}
type clientResp struct {
	kresp kmsg.Response
	err   error
}

func (c *Cluster) run() {
	for {
		var req clientReq
		select {
		case req = <-c.reqCh:
		case <-c.die:
			return
		}

		var (
			kreq  = req.kreq
			kresp kmsg.Response
			err   error
		)

		switch k := kmsg.Key(kreq.Key()); k {
		case kmsg.Produce:
			kresp, err = c.handleProduce(req.cc.b, kreq)
		case kmsg.ListOffsets:
			kresp, err = c.handleListOffsets(req.cc.b, kreq)
		case kmsg.Metadata:
			kresp, err = c.handleMetadata(kreq)
		case kmsg.ApiVersions:
			kresp, err = c.handleApiVersions(kreq)
		case kmsg.CreateTopics:
			kresp, err = c.handleCreateTopics(req.cc.b, kreq)
		case kmsg.InitProducerID:
			kresp, err = c.handleInitProducerID(kreq)
		default:
			err = fmt.Errorf("unahndled key %v", k)
		}

		if kresp == nil && err == nil { // produce request with no acks
			continue
		}

		select {
		case req.cc.respCh <- clientResp{kresp: kresp, err: err}:
		case <-c.die:
			return
		}
	}
}
