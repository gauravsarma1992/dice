package replication

import (
	"context"
	"log"
	"time"
)

type (
	BootstrapManager struct {
		ctx        context.Context
		remoteNode *Node
		replMgr    *ReplicationManager
	}
	// PingMessage
	PingRequest struct {
		Node *Node `json:"node"`
	}
	PingResponse struct {
		Node *Node `json:"node"`
	}

	// ClusterDiscovery
	ClusterDiscoveryRequest struct {
		Node *Node `json:"node"`
	}
	ClusterDiscoveryResponse struct {
		Nodes []*Node `json:"nodes"`
	}
)

func NewBootstrapManager(ctx context.Context) (bootstrapMgr *BootstrapManager, err error) {
	bootstrapMgr = &BootstrapManager{
		ctx: ctx,
	}
	bootstrapMgr.replMgr = ctx.Value(ReplicationManagerInContext).(*ReplicationManager)
	if bootstrapMgr.replMgr.localNode, err = NewNode(bootstrapMgr.ctx, bootstrapMgr.replMgr.config.NodeConfig); err != nil {
		return
	}
	if err = bootstrapMgr.replMgr.localNode.Boot(); err != nil {
		return
	}
	return
}

func (bootstrapMgr *BootstrapManager) PingHandler(reqMsg *Message) (respMsg *Message, err error) {
	// Get the local node ID from the context
	reqMsg.Remote.NodeID = bootstrapMgr.replMgr.localNode.ID

	respMsg = NewMessage(
		InfoMessageGroup,
		PingMessageType,
		reqMsg.Remote,
		reqMsg.Local,
		&PingResponse{Node: bootstrapMgr.replMgr.localNode},
	)
	return
}

func (bootstrapMgr *BootstrapManager) ClusterDiscoveryHandler(reqMsg *Message) (respMsg *Message, err error) {
	var (
		nodes               []*Node
		remoteNode          *Node
		clusterDiscoveryMsg *ClusterDiscoveryRequest
	)
	// Get the cluster nodes
	nodes = bootstrapMgr.replMgr.cluster.GetNodes()

	clusterDiscoveryMsg = &ClusterDiscoveryRequest{}
	if err = reqMsg.FillValue(clusterDiscoveryMsg); err != nil {
		return
	}
	remoteNode = clusterDiscoveryMsg.Node
	// Adding the node in the remote node's cluster
	if err = bootstrapMgr.replMgr.cluster.Update([]*Node{remoteNode}); err != nil {
		return
	}
	respMsg = NewMessage(
		InfoMessageGroup,
		ClusterDiscoveryMessageType,
		reqMsg.Remote,
		bootstrapMgr.replMgr.localNode.GetLocalUser(),
		&ClusterDiscoveryResponse{Nodes: nodes},
	)
	return
}

func (bootstrapMgr *BootstrapManager) GetClusterInfo(remoteNode *Node) (err error) {
	var (
		respMsg *Message
	)
	localNode := bootstrapMgr.replMgr.localNode

	clusterDiscoveryMsg := NewMessage(
		InfoMessageGroup,
		ClusterDiscoveryMessageType,
		localNode.GetLocalUser(),
		remoteNode.GetLocalUser(),
		&ClusterDiscoveryRequest{Node: localNode},
	)
	if respMsg, err = bootstrapMgr.replMgr.transportMgr.Send(clusterDiscoveryMsg); err != nil {
		return
	}
	clusterDiscoveryResp := &ClusterDiscoveryResponse{}
	if err = respMsg.FillValue(clusterDiscoveryResp); err != nil {
		return
	}
	// Adding the node in the local node's cluster
	if err = bootstrapMgr.replMgr.cluster.Update(clusterDiscoveryResp.Nodes); err != nil {
		return
	}
	return
}

func (bootstrapMgr *BootstrapManager) start() (err error) {
	ticker := time.NewTicker(10 * time.Second)

	for {
		select {
		case <-bootstrapMgr.ctx.Done():
			return
		case <-ticker.C:
			if err = bootstrapMgr.GetClusterInfo(bootstrapMgr.remoteNode); err != nil {
				return
			}
		}
	}

	return
}

func (bootstrapMgr *BootstrapManager) Start() (err error) {
	if bootstrapMgr.remoteNode, err = bootstrapMgr.replMgr.localNode.ConnectToRemoteNode(); err != nil {
		return
	}
	go func() {
		if err = bootstrapMgr.start(); err != nil {
			log.Println("Error in bootstrap manager", err)
			return
		}
	}()
	return
}
