package kfake

import (
	"github.com/twmb/franz-go/pkg/kmsg"
)

func init() { regKey(0, 4, 13) }

func (c *Cluster) handleFetch(b *broker, kreq kmsg.Request) (kmsg.Response, error) {
	var (
		req   = kreq.(*kmsg.FetchRequest)
		resp  = req.ResponseKind().(*kmsg.FetchResponse)
		tdone = make(map[string][]kmsg.FetchResponseTopicPartition)
	)

	if err := checkReqVersion(req.Key(), req.Version); err != nil {
		return nil, err
	}

	id2t := make(map[uuid]string)
	tidx := make(map[string]int)

	donet := func(t string, id uuid, errCode int16) *kmsg.FetchResponseTopic {
		if i, ok := tidx[t]; ok {
			return &resp.Topics[i]
		}
		id2t[id] = t
		tidx[t] = len(resp.Topics)
		st := kmsg.NewFetchResponseTopic()
		st.Topic = t
		st.TopicID = id
		resp.Topics = append(resp.Topics, st)
		return &resp.Topics[len(resp.Topics)-1]
	}
	donep := func(t string, id uuid, p int32, errCode int16) *kmsg.FetchResponseTopicPartition {
		sp := kmsg.NewFetchResponseTopicPartition()
		sp.Partition = p
		sp.ErrorCode = errCode
		st := donet(t, id, 0)
		st.Partitions = append(st.Partitions, sp)
		return &st.Partitions[len(st.Partitions)-1]
	}

	for _, rt := range req.Topics {
		for _, rp := range rt.Partitions {
		}
	}

	return toresp(), nil
}
