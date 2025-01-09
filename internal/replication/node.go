package replication

import (
	"context"
	"time"
)

const (
	// Node phases
	BootstrapNodePhase       NodePhaseT = NodePhaseT(0)
	DataReadinessNodePhase   NodePhaseT = NodePhaseT(1)
	DataReplicationNodePhase NodePhaseT = NodePhaseT(2)

	// Node state or health
	NodeStateHealthy   NodeStateT = NodeStateT(0)
	NodeStateUnhealthy NodeStateT = NodeStateT(1)
	NodeStateUnknown   NodeStateT = NodeStateT(2)

	// Node types
	NodeTypeLeader   NodeTypeT = NodeTypeT(0)
	NodeTypeFollower NodeTypeT = NodeTypeT(1)
	NodeTypeHidden   NodeTypeT = NodeTypeT(2)
)

type (
	NodeID     uint64
	NodePhaseT uint8
	NodeStateT uint8
	NodeTypeT  uint8

	Node struct {
		ctx context.Context
		ID  NodeID

		// Runtime information of the state, phase and type of the node.
		state    NodeStateT `json:"state"`
		phase    NodePhaseT `json:"phase"`
		nodeType NodeTypeT  `json:"node_type"`

		transportManager *TransportManager
		config           *NodeConfig
	}

	NodeConfig struct {
		LocalHost string `json:"local_host"`
		LocalPort int    `json:"local_port"`

		// Remote host is the host of the node that we are replicating from.
		// If the remote host is the same as the local host or if the remote host
		// is not provided, then the node is a leader node or a single node.
		// The remote node doesn't have to be the leader. It can recursively learn
		// about the leader from the remote node.
		RemoteHost string `json:"remote_host"`
		RemotePort int    `json:"remote_port"`
	}
)

func DefaultSingleNodeConfig() (config *NodeConfig) {
	config = &NodeConfig{
		LocalHost: "127.0.0.1",
		LocalPort: 8080,
	}
	return
}

func NewNode(ctx context.Context, config *NodeConfig) (node *Node, err error) {
	if config == nil {
		config = DefaultSingleNodeConfig()
	}
	node = &Node{
		ID:               NodeID(time.Now().UnixNano()),
		ctx:              ctx,
		config:           config,
		state:            NodeStateUnknown,
		phase:            BootstrapNodePhase,
		nodeType:         NodeTypeHidden,
		transportManager: node.ctx.Value(TransportManagerInContext).(*TransportManager),
	}
	return
}

func (node *Node) verifyConfig() (err error) {
	// check if local network config is correct
	// check if remote network config is correct
	// check current server resources
	return
}

func (node *Node) Boot() (err error) {
	// Start the node
	if err = node.verifyConfig(); err != nil {
		return
	}
	return
}

func (node *Node) ConnectToRemoteNode() (nodes []*Node, err error) {
	var (
		respMsg      *Message
		remoteNodeID NodeID
	)
	if remoteNodeID, err = node.transportManager.ConvertIpToNode(node.config.RemoteHost); err != nil {
		return
	}
	clusterDiscoveryMsg := NewMessage(
		InfoMessageGroup,
		ClusterDiscoveryMessageType,
		node.ID,
		remoteNodeID,
		&ClusterDiscoveryRequest{Node: node},
	)
	if respMsg, err = node.transportManager.Send(clusterDiscoveryMsg); err != nil {
		return
	}
	nodes = respMsg.Value.(*ClusterDiscoveryResponse).Nodes
	return
}

func (node *Node) Stop() (err error) {
	// Stop the node
	return
}
