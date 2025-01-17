package replication

import (
	"encoding/json"
	"fmt"
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
	HeartbeatMessageType        MessageTypeT = MessageTypeT(2)
)

type (
	MessageID     uint64
	MessageGroupT uint8
	MessageTypeT  uint16

	MessageHandler func(*Message) (*Message, error)

	Message struct {
		Version uint64    `json:"version"`
		ID      MessageID `json:"id"`

		Timestamp uint64 `json:"timestamp"`

		LocalNodeID  NodeID `json:"local_node_id"`
		RemoteNodeID NodeID `json:"remote_node_id"`

		// The group of the message.
		Group MessageGroupT `json:"group"`
		Type  MessageTypeT  `json:"message_type"`
		Value []byte        `json:"value"`
	}
)

func NewMessage(group MessageGroupT, msgType MessageTypeT, localNodeID NodeID, remoteNodeID NodeID, value interface{}) (msg *Message) {
	msgVal, _ := json.Marshal(value)
	msg = &Message{
		ID:        MessageID(time.Now().UnixNano()),
		Version:   1,
		Timestamp: uint64(time.Now().UnixNano()),

		LocalNodeID:  localNodeID,
		RemoteNodeID: remoteNodeID,

		Group: group,
		Type:  msgType,
		Value: msgVal,
	}
	return
}

func (msg *Message) String() string {
	return fmt.Sprintf(
		"ID: %d, Group: %d, Type: %d, LocalNodeID: %d, RemoteNodeID: %d, Value: %s",
		msg.ID, msg.Group, msg.Type, msg.LocalNodeID, msg.RemoteNodeID, msg.Value,
	)
}

func (msg *Message) FillValue(msgVal interface{}) (err error) {
	err = json.Unmarshal(msg.Value, msgVal)
	return
}
