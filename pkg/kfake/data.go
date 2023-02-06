package kfake

import (
	"crypto/sha256"
	"math/rand"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// TODO
//
// * Write to disk, if configured.

var noID uuid

type (
	uuid [16]byte

	data struct {
		c *Cluster
		m map[string]map[int32]*partData

		id2t map[uuid]string // topic IDs => topic name
		t2id map[string]uuid // topic name => topic IDs
	}

	partData struct {
		batches []kmsg.RecordBatch

		highWatermark    int64
		lastStableOffset int64
		logStartOffset   int64

		// abortedTxns
		rf     int8
		leader *broker
	}
)

func (d *data) mkt(t string, nparts int) {
	if _, exists := d.m[t]; exists {
		panic("should have checked existence already")
	}
	var id uuid
	for {
		sha := sha256.Sum256([]byte(strconv.Itoa(int(time.Now().UnixNano()))))
		copy(id[:], sha[:])
		if _, exists := d.id2t[id]; !exists {
			break
		}
	}

	if nparts < 0 {
		nparts = d.c.cfg.defaultNumParts
	}
	d.id2t[id] = t
	d.t2id[t] = id
	d.m[t] = make(map[int32]*partData)
	leaderAt := rand.Uint64()
	for i := 0; i < nparts; i++ {
		leader := d.c.bs[leaderAt%uint64(len(d.c.bs))]
		leaderAt++
		d.mkp(t, int32(i), leader)
	}
}

// Makes a topic partition, returning true if the partition was made or false
// if the partition already existed.
func (d *data) mkp(t string, p int32, leader *broker) bool {
	ps := d.m[t]
	if ps == nil {
		ps = make(map[int32]*partData)
		d.m[t] = ps
	}
	pd := ps[p]
	if pd == nil {
		pd := &partData{
			leader: leader,
		}
		ps[p] = pd
		return true
	}
	return false
}

func (d *data) getp(t string, p int32) (*partData, bool) {
	ps := d.m[t]
	if ps == nil {
		return nil, false
	}
	pd := ps[p]
	return pd, pd != nil
}

func (pd *partData) pushBatch(b kmsg.RecordBatch) {
	pd.batches = append(pd.batches, b)
	pd.highWatermark += int64(b.NumRecords)
	pd.lastStableOffset += int64(b.NumRecords) // TODO
}
