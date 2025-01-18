package replication

import (
	"context"
	"fmt"
	"log"
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

var (
	SameNodeError = fmt.Errorf("remote node is the same as the local node")
)

type (
	NodeID     uint64
	NodePhaseT uint8
	NodeStateT uint8
	NodeTypeT  uint8

	Node struct {
		ctx context.Context
		ID  NodeID

		// Runtime information of the State, phase and type of the node.
		State    NodeStateT `json:"state"`
		Phase    NodePhaseT `json:"phase"`
		NodeType NodeTypeT  `json:"node_type"`

		transportManager *TransportManager
		replMgr          *ReplicationManager
		Config           *NodeConfig
	}
	NodeAddr struct {
		Host string `json:"host"`
		Port string `json:"port"`
	}

	NodeConfig struct {
		Local *NodeAddr `json:"local"`

		// Remote host is the host of the node that we are replicating from.
		// If the remote host is the same as the local host or if the remote host
		// is not provided, then the node is a leader node or a single node.
		// The remote node doesn't have to be the leader. It can recursively learn
		// about the leader from the remote node.
		Remote *NodeAddr `json:"remote"`
	}
)

func (node *Node) String() string {
	return fmt.Sprintf(
		"ID: %d, State: %d, Phase: %d, Type: %d, Config: %s",
		node.ID, node.State, node.Phase, node.NodeType, node.Config,
	)
}

func (nodeConfig *NodeConfig) String() string {
	return fmt.Sprintf(
		"LocalHost: %s, LocalPort: %s, RemoteHost: %s, RemotePort: %s",
		nodeConfig.Local.Host,
		nodeConfig.Local.Port,
		nodeConfig.Remote.Host,
		nodeConfig.Remote.Port,
	)
}

func DefaultSingleNodeConfig() (config *NodeConfig) {
	config = &NodeConfig{
		Local: &NodeAddr{
			Host: "127.0.0.1",
			Port: "8080",
		},
		Remote: &NodeAddr{},
	}
	return
}

func NewNode(ctx context.Context, config *NodeConfig) (node *Node, err error) {
	if config == nil {
		config = DefaultSingleNodeConfig()
	}
	node = &Node{
		ID:       NodeID(time.Now().UnixNano()),
		ctx:      ctx,
		Config:   config,
		State:    NodeStateUnknown,
		Phase:    BootstrapNodePhase,
		NodeType: NodeTypeHidden,
	}
	node.replMgr = ctx.Value(ReplicationManagerInContext).(*ReplicationManager)
	node.transportManager = node.replMgr.transportMgr
	return
}

func (node *Node) verifyConfig() (err error) {
	// check if local network config is correct
	if node.Config.Local.Host == "" || node.Config.Local.Port == "" {
		err = fmt.Errorf("local host or port is not provided for node %d", node.ID)
		return
	}
	// check if remote network config is correct
	if node.Config.Remote.Host == "" {
		err = fmt.Errorf("remote host is not provided for node %d", node.ID)
		return
	}
	// check current server resources
	return
}

func (node *Node) Boot() (err error) {
	// Start the node
	log.Println("Node is booting. Config:", node.ID, node.Config)
	if err = node.verifyConfig(); err != nil {
		return
	}
	return
}

func (node *Node) GetLocalUser() (msgUser *MessageUser) {
	msgUser = &MessageUser{
		NodeID: node.ID,
		Addr:   node.Config.Local,
	}
	return
}

func (node *Node) ConnectToRemoteNode() (nodes []*Node, err error) {
	var (
		respMsg    *Message
		remoteNode *Node
	)
	if remoteNode, err = node.transportManager.ConnectToNode(node.Config.Remote); err != nil {
		return
	}
	if node.ID == remoteNode.ID {
		log.Println(SameNodeError, node.ID, remoteNode.ID)
		return
	}
	clusterDiscoveryMsg := NewMessage(
		InfoMessageGroup,
		ClusterDiscoveryMessageType,
		node.GetLocalUser(),
		remoteNode.GetLocalUser(),
		&ClusterDiscoveryRequest{Node: node},
	)
	if respMsg, err = node.transportManager.Send(clusterDiscoveryMsg); err != nil {
		return
	}
	clusterDiscoveryResp := &ClusterDiscoveryResponse{}
	if err = respMsg.FillValue(clusterDiscoveryResp); err != nil {
		return
	}
	nodes = clusterDiscoveryResp.Nodes
	return
}

func (node *Node) Stop() (err error) {
	// Stop the node
	return
}
