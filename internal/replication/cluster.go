package replication

import (
	"context"
	"log"
	"sync"
)

type (
	Cluster struct {
		ctx context.Context

		localNode  *Node
		leaderNode *Node

		nodes map[NodeID]*Node

		clusterLock *sync.RWMutex
	}
)

func NewCluster(ctx context.Context, localNode *Node) (cluster *Cluster, err error) {
	cluster = &Cluster{
		ctx:         ctx,
		localNode:   localNode,
		clusterLock: &sync.RWMutex{},
	}
	return
}

func (cluster *Cluster) AddNode(node *Node) (err error) {
	cluster.clusterLock.Lock()
	defer cluster.clusterLock.Unlock()

	// Skip if the node is the local node
	if node.ID == cluster.localNode.ID {
		return
	}
	cluster.nodes[node.ID] = node
	return
}

func (cluster *Cluster) RemoveNode(node *Node) (err error) {
	cluster.clusterLock.Lock()
	defer cluster.clusterLock.Unlock()

	// Skip if the node is the local node
	if node.ID == cluster.localNode.ID {
		return
	}
	cluster.nodes[node.ID] = node
	delete(cluster.nodes, node.ID)
	return
}

func (cluster *Cluster) Update(receivedNodes []*Node) (err error) {
	// TODO: Remove deleted nodes
	for _, node := range receivedNodes {
		if err = cluster.AddNode(node); err != nil {
			log.Printf("unable to add node %d to the cluster", node.ID)
			continue
		}
	}
	return
}
