package replication

import (
	"context"
	"log"
	"time"
)

type (
	HeartbeatManager struct {
		ctx     context.Context
		replMgr *ReplicationManager
	}
	HeartbeatRequestMsg struct {
		Node *Node `json:"node"`
	}
	HeartbeatResponseMsg struct {
		NodeID NodeID `json:"node_id"`
	}
)

func NewHeartbeatManager(ctx context.Context) (hbMgr *HeartbeatManager, err error) {
	hbMgr = &HeartbeatManager{
		ctx: ctx,
	}
	hbMgr.replMgr = ctx.Value(ReplicationManagerInContext).(*ReplicationManager)
	return
}

func (hbMgr *HeartbeatManager) HeartbeatHandler(reqMsg *Message) (respMsg *Message, err error) {
	respMsg = NewMessage(
		InfoMessageGroup,
		HeartbeatMessageType,
		reqMsg.Remote,
		reqMsg.Local,
		&HeartbeatResponseMsg{NodeID: reqMsg.Remote.NodeID},
	)
	return
}

func (hbMgr *HeartbeatManager) sendHeartbeat() (respMsg *Message, err error) {
	var (
		nodes []*Node
	)
	if nodes, err = hbMgr.replMgr.cluster.GetNodes(); err != nil {
		return
	}
	for _, node := range nodes {
		if node.ID == hbMgr.replMgr.localNode.ID {
			continue
		}
		heartbeatReqMsg := NewMessage(
			InfoMessageGroup,
			HeartbeatMessageType,
			hbMgr.replMgr.localNode.GetLocalUser(),
			node.GetLocalUser(),
			&HeartbeatRequestMsg{
				Node: hbMgr.replMgr.localNode,
			},
		)
		heartbeatRespMsg := &Message{}
		log.Println("Sending heartbeats message from node", hbMgr.replMgr.localNode, heartbeatReqMsg)
		if heartbeatRespMsg, err = hbMgr.replMgr.transportMgr.Send(heartbeatReqMsg); err != nil {
			return
		}
		log.Println(heartbeatRespMsg)
	}
	return
}

func (hbMgr *HeartbeatManager) Start() (err error) {
	ticker := time.NewTicker(5 * time.Second)
	for {
		select {
		case <-hbMgr.ctx.Done():
			return
		case <-ticker.C:

			if _, err = hbMgr.sendHeartbeat(); err != nil {
				log.Println("error in sending heartbeat", err)
				return
			}
		}
	}
	return
}
