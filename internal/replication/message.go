package replication

import (
	"time"
)

const (
	// Message groups
	ReplicationMessageGroup MessageGroupT = MessageGroupT(0)
	InfoMessageGroup        MessageGroupT = MessageGroupT(1)
	HeartbeatMessageGroup   MessageGroupT = MessageGroupT(2)

	// Message Types
	PingMessageType             MessageTypeT = MessageTypeT(0)
	ClusterDiscoveryMessageType MessageTypeT = MessageTypeT(1)
)

type (
	MessageID     uint64
	MessageGroupT uint8
	MessageTypeT  uint16

	MessageHandler func(*Message) (*Message, error)

	Message struct {
		Version uint64
		ID      MessageID

		Timestamp uint64

		LocalNodeID  NodeID
		RemoteNodeID NodeID

		// The group of the message.
		Group MessageGroupT
		Type  MessageTypeT
		Value interface{}
	}

	// ClusterDiscovery
	ClusterDiscoveryRequest struct {
		Node *Node
	}
	ClusterDiscoveryResponse struct {
		Nodes []*Node
	}
)

func NewMessage(group MessageGroupT, msgType MessageTypeT, localNodeID NodeID, remoteNodeID NodeID, value interface{}) (msg *Message) {
	msg = &Message{
		ID:        MessageID(time.Now().UnixNano()),
		Version:   1,
		Timestamp: uint64(time.Now().UnixNano()),

		LocalNodeID:  localNodeID,
		RemoteNodeID: remoteNodeID,

		Group: group,
		Value: value,
	}
	return
}

func (replMgr *ReplicationManager) PingHandler(reqMsg *Message) (respMsg *Message, err error) {
	// Get the local node ID from the context
	localNodeID := replMgr.ctx.Value(LocalNodeInContext).(*Node).ID
	respMsg = NewMessage(
		InfoMessageGroup,
		PingMessageType,
		reqMsg.RemoteNodeID,
		localNodeID,
		&Node{ID: localNodeID},
	)
	return
}

func (replMgr *ReplicationManager) ClusterDiscoveryHandler(reqMsg *Message) (respMsg *Message, err error) {
	return
}
