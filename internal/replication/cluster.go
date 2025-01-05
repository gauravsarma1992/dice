package replication

import (
	"context"
	"sync"
)

type (
	Cluster struct {
		ctx context.Context

		localNode  *Node
		leaderNode *Node

		Nodes map[NodeID]*Node

		clusterLock *sync.RWMutex
	}
)

func NewCluster(ctx context.Context, localNode *Node) (cluster *Cluster, err error) {
	cluster = &Cluster{
		ctx:       ctx,
		localNode: localNode,

		clusterLock: &sync.RWMutex{},
	}
	return
}

func (cluster *Cluster) AddNode(node *Node) (err error) {
	cluster.clusterLock.Lock()
	defer cluster.clusterLock.Unlock()

	cluster.Nodes[node.ID] = node
	return
}
