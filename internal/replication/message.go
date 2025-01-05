package replication

import "time"

const (
	// Message groups
	ReplicationMessageGroup MessageGroupT = MessageGroupT(0)
	InfoMessageGroup        MessageGroupT = MessageGroupT(1)
	HeartbeatMessageGroup   MessageGroupT = MessageGroupT(2)
)

type (
	MessageID     uint64
	MessageGroupT uint8

	Message struct {
		Version uint64
		ID      MessageID

		Timestamp uint64

		LocalNodeID  NodeID
		RemoteNodeID NodeID

		// The group of the message.
		Group MessageGroupT
		Value interface{}
	}

	ClusterDiscoveryRequest struct {
		Node *Node
	}
	ClusterDiscoveryResponse struct {
		Nodes []*Node
	}
)

func NewMessage(group MessageGroupT, localNodeID NodeID, remoteNodeID NodeID, value interface{}) (msg *Message) {
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
