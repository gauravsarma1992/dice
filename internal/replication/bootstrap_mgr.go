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

func (bootstrapMgr *BootstrapManager) Start() (err error) {
	var (
		discoveredNodes []*Node
	)
	if discoveredNodes, err = bootstrapMgr.replMgr.localNode.ConnectToRemoteNode(); err != nil {
		return
	}
	// Adding the node in the local node's cluster
	if err = bootstrapMgr.replMgr.cluster.Update(discoveredNodes); err != nil {
		return
	}
	return
}
