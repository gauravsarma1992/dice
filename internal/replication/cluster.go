package replication

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

const ErrNoLeaderNode = "no leader node found"

type (
	Cluster struct {
		ctx context.Context

		leaderNode *Node
		nodes      map[NodeID]*Node

		clusterLock *sync.RWMutex

		replMgr *ReplicationManager

		lastLeaderChangedAt time.Time
		lastNodeAddedAt     time.Time
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

	// Skip if the node is the local node
	if node.ID == cluster.replMgr.localNode.ID {
		return
	}
	// Log if the node is not present in the cluster
	if localNodeCopy, isPresent := cluster.nodes[node.ID]; isPresent {
		if localNodeCopy.LastUpdatedAt.Equal(node.LastUpdatedAt) || localNodeCopy.LastUpdatedAt.After(node.LastUpdatedAt) {
			return
		}
	}
	cluster.replMgr.log.Println("Adding node to cluster", node)
	cluster.nodes[node.ID] = node
	cluster.lastNodeAddedAt = time.Now().UTC()

	if node.NodeType == NodeTypeLeader {
		if cluster.leaderNode != nil && node.ID == cluster.leaderNode.ID {
			return
		}
		if err = cluster.AddLeaderNode(node); err != nil {
			return
		}
		cluster.replMgr.log.Println("Leader node added to the cluster", node)
	}

	return
}

func (cluster *Cluster) GetLeaderNode() (node *Node, err error) {
	cluster.clusterLock.RLock()
	defer cluster.clusterLock.RUnlock()

	node = cluster.leaderNode
	if node == nil {
		err = fmt.Errorf(ErrNoLeaderNode)
	}
	return
}

func (cluster *Cluster) AddLeaderNode(leaderNode *Node) (err error) {
	cluster.clusterLock.Lock()
	defer cluster.clusterLock.Unlock()

	cluster.leaderNode = leaderNode
	cluster.lastLeaderChangedAt = time.Now().UTC()

	return
}

func (cluster *Cluster) GetRemoteNodes() (nodes []*Node) {
	cluster.clusterLock.RLock()
	defer cluster.clusterLock.RUnlock()

	for _, node := range cluster.nodes {
		if node.ID == cluster.replMgr.localNode.ID {
			continue
		}
		nodes = append(nodes, node)
	}
	return
}

func (cluster *Cluster) GetNodes() (nodes []*Node) {
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
	if node.ID == cluster.replMgr.localNode.ID {
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
