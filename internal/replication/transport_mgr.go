package replication

import (
	"context"
	"sync"

	"errors"
)

var (
	ErrTransportClientNotFoundError = errors.New("transport client not found")
	NodeNotPresentInTransportError  = errors.New("node is not present in the transport manager")
)

type (
	TransportManager struct {
		ctx context.Context

		transportClients map[NodeID]Transport
		ipToNodeStore    map[string]NodeID
		nodeToIpStore    map[NodeID]string

		msgHandlers map[MessageTypeT]MessageHandler

		clientLock *sync.RWMutex
	}
)

func NewTransportManager(ctx context.Context) (transportManager *TransportManager, err error) {
	transportManager = &TransportManager{
		ctx:              ctx,
		transportClients: make(map[NodeID]Transport, 10),
		ipToNodeStore:    make(map[string]NodeID, 10),
		nodeToIpStore:    make(map[NodeID]string, 10),
		clientLock:       &sync.RWMutex{},
	}
	if err = transportManager.setupMsgHandlers(); err != nil {
		return
	}
	return
}

func (transportManager *TransportManager) setupMsgHandlers() (err error) {
	replMgr := transportManager.ctx.Value(ReplicationManagerInContext).(*ReplicationManager)
	transportManager.msgHandlers[PingMessageType] = replMgr.PingHandler
	transportManager.msgHandlers[ClusterDiscoveryMessageType] = replMgr.ClusterDiscoveryHandler
	return
}

func (transportManager *TransportManager) ConvertIpToNode(remoteIp string) (nodeID NodeID, err error) {
	isPresent := true

	transportManager.clientLock.RLock()
	defer transportManager.clientLock.RUnlock()

	if nodeID, isPresent = transportManager.ipToNodeStore[remoteIp]; isPresent {
		return
	}
	return
}

func (transportManager *TransportManager) ConvertNodeToIp(remoteNodeID NodeID) (remoteIp string, err error) {
	isPresent := true

	transportManager.clientLock.RLock()
	defer transportManager.clientLock.RUnlock()

	if remoteIp, isPresent = transportManager.nodeToIpStore[remoteNodeID]; isPresent {
		return
	}
	return
}

func (transportManager *TransportManager) ConnectToNode(remoteIp string) (remoteNode *Node, err error) {
	var (
		transport Transport
		pingResp  *Message
	)
	if transport, err = NewTransport(transportManager.ctx); err != nil {
		return
	}
	if pingResp, err = transport.Ping(remoteIp); err != nil {
		return
	}
	remoteNode = pingResp.Value.(*Node)
	if err = transportManager.AddTransportClient(remoteNode.ID, transport); err != nil {
		return
	}
	transportManager.updateIpToNodeMapping(remoteIp, remoteNode.ID)
	transportManager.updateNodeToIpMapping(remoteNode.ID, remoteIp)
	return
}

func (transportManager *TransportManager) updateIpToNodeMapping(remoteIp string, remoteNodeID NodeID) (err error) {
	transportManager.clientLock.Lock()
	defer transportManager.clientLock.Unlock()

	transportManager.ipToNodeStore[remoteIp] = remoteNodeID
	return
}

func (transportManager *TransportManager) updateNodeToIpMapping(remoteNodeID NodeID, remoteIp string) (err error) {
	transportManager.clientLock.Lock()
	defer transportManager.clientLock.Unlock()

	transportManager.nodeToIpStore[remoteNodeID] = remoteIp
	return
}

func (transportManager *TransportManager) AddTransportClient(nodeID NodeID, transport Transport) (err error) {
	transportManager.clientLock.Lock()
	defer transportManager.clientLock.Unlock()

	transportManager.transportClients[nodeID] = transport
	return
}

func (transportManager *TransportManager) GetTransportClient(nodeID NodeID) (transport Transport, err error) {
	transportManager.clientLock.RLock()
	defer transportManager.clientLock.RUnlock()

	transport, ok := transportManager.transportClients[nodeID]
	if !ok {
		err = ErrTransportClientNotFoundError
		return
	}
	return
}

func (transportManager *TransportManager) Send(reqMsg *Message) (respMsg *Message, err error) {
	var (
		remoteNodeTransport Transport
	)
	if remoteNodeTransport, err = transportManager.GetTransportClient(reqMsg.RemoteNodeID); err != nil {
		return
	}
	if respMsg, err = remoteNodeTransport.Send(reqMsg); err != nil {
		return
	}
	return
}
