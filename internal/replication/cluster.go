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

		replMgr *ReplicationManager
	}
)

func NewCluster(ctx context.Context) (cluster *Cluster, err error) {
	cluster = &Cluster{
		ctx:         ctx,
		clusterLock: &sync.RWMutex{},
		nodes:       make(map[NodeID]*Node, 10),
	}
	cluster.replMgr = ctx.Value(ReplicationManagerInContext).(*ReplicationManager)
	return
}

func (cluster *Cluster) AddNode(node *Node) (err error) {
	cluster.clusterLock.Lock()
	defer cluster.clusterLock.Unlock()

	if cluster.localNode == nil {
		cluster.localNode = cluster.replMgr.localNode
	}
	log.Println("Adding node to cluster", node.ID)

	// Skip if the node is the local node
	if node.ID == cluster.localNode.ID {
		return
	}
	cluster.nodes[node.ID] = node
	return
}

func (cluster *Cluster) GetNodes() (nodes []*Node, err error) {
	cluster.clusterLock.RLock()
	defer cluster.clusterLock.RUnlock()

	for _, node := range cluster.nodes {
		nodes = append(nodes, node)
	}
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
