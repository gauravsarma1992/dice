package replication

import (
	"context"
)

type (
	BootstrapManager struct {
		ctx     context.Context
		replMgr *ReplicationManager
	}
	// PingMessage
	PingRequest struct {
		Node *Node `json:"node"`
	}
	PingResponse struct {
		NodeID NodeID `json:"node_id"`
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
	return
}

func (bootstrapMgr *BootstrapManager) PingHandler(reqMsg *Message) (respMsg *Message, err error) {
	// Get the local node ID from the context
	respMsg = NewMessage(
		InfoMessageGroup,
		PingMessageType,
		reqMsg.RemoteNodeID,
		bootstrapMgr.replMgr.localNode.ID,
		&PingResponse{NodeID: bootstrapMgr.replMgr.localNode.ID},
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
	if nodes, err = bootstrapMgr.replMgr.cluster.GetNodes(); err != nil {
		return
	}
	clusterDiscoveryMsg = &ClusterDiscoveryRequest{}
	if err = reqMsg.FillValue(clusterDiscoveryMsg); err != nil {
		return
	}
	remoteNode = clusterDiscoveryMsg.Node
	if err = bootstrapMgr.replMgr.cluster.Update([]*Node{remoteNode}); err != nil {
		return
	}
	respMsg = NewMessage(
		InfoMessageGroup,
		ClusterDiscoveryMessageType,
		reqMsg.RemoteNodeID,
		bootstrapMgr.replMgr.localNode.ID,
		&ClusterDiscoveryResponse{Nodes: nodes},
	)
	return
}

func (bootstrapMgr *BootstrapManager) Start() (err error) {
	var (
		discoveredNodes []*Node
	)
	if bootstrapMgr.replMgr.localNode, err = NewNode(bootstrapMgr.ctx, bootstrapMgr.replMgr.config.NodeConfig); err != nil {
		return
	}
	if err = bootstrapMgr.replMgr.localNode.Boot(); err != nil {
		return
	}
	if discoveredNodes, err = bootstrapMgr.replMgr.localNode.ConnectToRemoteNode(); err != nil {
		return
	}
	if err = bootstrapMgr.replMgr.cluster.Update(discoveredNodes); err != nil {
		return
	}
	return
}
