package replication

import (
	"context"
	"log"
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
		ipToNodeStore    map[*NodeAddr]NodeID
		nodeToAddrStore  map[NodeID]*NodeAddr

		msgHandlers map[MessageTypeT]MessageHandler

		clientLock *sync.RWMutex
		replMgr    *ReplicationManager
	}
)

func NewTransportManager(ctx context.Context) (transportManager *TransportManager, err error) {
	transportManager = &TransportManager{
		ctx:              ctx,
		transportClients: make(map[NodeID]Transport, 10),
		ipToNodeStore:    make(map[*NodeAddr]NodeID, 10),
		nodeToAddrStore:  make(map[NodeID]*NodeAddr, 10),
		msgHandlers:      make(map[MessageTypeT]MessageHandler, 10),
		clientLock:       &sync.RWMutex{},
	}
	transportManager.replMgr = ctx.Value(ReplicationManagerInContext).(*ReplicationManager)
	if err = transportManager.setupMsgHandlers(); err != nil {
		return
	}
	return
}

func (transportManager *TransportManager) setupMsgHandlers() (err error) {
	transportManager.msgHandlers[PingMessageType] = transportManager.replMgr.bootstrapMgr.PingHandler
	transportManager.msgHandlers[ClusterDiscoveryMessageType] = transportManager.replMgr.bootstrapMgr.ClusterDiscoveryHandler
	transportManager.msgHandlers[HeartbeatMessageType] = transportManager.replMgr.hbMgr.HeartbeatHandler
	return
}

func (transportManager *TransportManager) ConvertIpToNode(remoteAddr *NodeAddr) (nodeID NodeID, err error) {
	isPresent := true

	transportManager.clientLock.RLock()
	defer transportManager.clientLock.RUnlock()

	if nodeID, isPresent = transportManager.ipToNodeStore[remoteAddr]; isPresent {
		return
	}
	return
}

func (transportManager *TransportManager) ConvertNodeToAddr(remoteNodeID NodeID) (remoteAddr *NodeAddr, err error) {
	isPresent := true

	transportManager.clientLock.RLock()
	defer transportManager.clientLock.RUnlock()

	if remoteAddr, isPresent = transportManager.nodeToAddrStore[remoteNodeID]; isPresent {
		return
	}
	return
}

func (transportManager *TransportManager) ConnectToNode(remoteAddr *NodeAddr) (remoteNode *Node, err error) {
	var (
		transport   Transport
		pingRespMsg *Message
		pingResp    *PingResponse
	)
	if transport, err = NewTransport(transportManager.ctx, transportManager.replMgr.localNode); err != nil {
		return
	}
	if pingRespMsg, err = transport.Ping(remoteAddr); err != nil {
		return
	}
	pingResp = &PingResponse{}
	if err = pingRespMsg.FillValue(pingResp); err != nil {
		return
	}
	remoteNode = pingResp.Node
	log.Println("Connected to remote node", pingRespMsg, remoteNode)

	if err = transportManager.AddTransportClient(remoteNode, remoteAddr, transport); err != nil {
		return
	}
	return
}

func (transportManager *TransportManager) updateIpToNodeMapping(remoteAddr *NodeAddr, remoteNodeID NodeID) (err error) {
	transportManager.clientLock.Lock()
	defer transportManager.clientLock.Unlock()

	transportManager.ipToNodeStore[remoteAddr] = remoteNodeID
	return
}

func (transportManager *TransportManager) updateNodeToIpMapping(remoteNodeID NodeID, remoteAddr *NodeAddr) (err error) {
	transportManager.clientLock.Lock()
	defer transportManager.clientLock.Unlock()

	transportManager.nodeToAddrStore[remoteNodeID] = remoteAddr
	return
}

func (transportManager *TransportManager) AddTransportClient(node *Node, remoteAddr *NodeAddr, transport Transport) (err error) {
	transportManager.clientLock.Lock()
	transportManager.transportClients[node.ID] = transport
	transportManager.clientLock.Unlock()

	transportManager.updateIpToNodeMapping(remoteAddr, node.ID)
	transportManager.updateNodeToIpMapping(node.ID, remoteAddr)

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

func (transportManager *TransportManager) CreateAndGetTransportClient(nodeID NodeID) (transport Transport, err error) {
	return
}

func (transportManager *TransportManager) Send(reqMsg *Message) (respMsg *Message, err error) {
	var (
		remoteNodeTransport Transport
	)
	if remoteNodeTransport, err = transportManager.GetTransportClient(reqMsg.Remote.NodeID); err != nil {
		log.Println("transport client not found, creating transport client", err)
		if remoteNodeTransport, err = transportManager.CreateAndGetTransportClient(reqMsg.Remote.NodeID); err != nil {
			return
		}
		return
	}
	if respMsg, err = remoteNodeTransport.Send(reqMsg); err != nil {
		return
	}
	return
}
