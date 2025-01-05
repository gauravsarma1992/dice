package replication

import (
	"context"
)

const (
	ReplicationManagerInContext = "replication-manager-in-context"
	TransportManagerInContext   = "transport-manager-in-context"
	LocalNodeInContext          = "local-node-in-context"
)

type (
	ReplicationManager struct {
		ctx        context.Context
		cancelFunc context.CancelFunc

		localNode *Node
		cluster   *Cluster

		transportMgr *TransportManager
		config       *ReplicationConfig
	}

	ReplicationConfig struct{}
)

func NewReplicationManager(config *ReplicationConfig) (replMgr *ReplicationManager, err error) {
	replMgr = &ReplicationManager{
		config: config,
	}
	replMgr.ctx, replMgr.cancelFunc = context.WithCancel(context.Background())

	if replMgr.transportMgr, err = NewTransportManager(replMgr.ctx); err != nil {
		return
	}
	if replMgr.cluster, err = NewCluster(replMgr.ctx, replMgr.localNode); err != nil {
		return
	}
	replMgr.ctx = context.WithValue(replMgr.ctx, ReplicationManagerInContext, replMgr)
	replMgr.ctx = context.WithValue(replMgr.ctx, TransportManagerInContext, replMgr)
	return
}

func (replMgr *ReplicationManager) StartNetworkConnectivity() (err error) {
	return
}

func (replMgr *ReplicationManager) StartBootstrapPhase() (err error) {
	var (
		discoveredNodes []*Node
	)
	if replMgr.localNode, err = NewNode(replMgr.ctx, nil); err != nil {
		return
	}
	replMgr.ctx = context.WithValue(replMgr.ctx, LocalNodeInContext, replMgr.localNode)
	if err = replMgr.localNode.Boot(); err != nil {
		return
	}
	if discoveredNodes, err = replMgr.localNode.ConnectToRemoteNode(); err != nil {
		return
	}
	if err = replMgr.cluster.Update(discoveredNodes); err != nil {
		return
	}
	return
}

func (replMgr *ReplicationManager) Run() (err error) {
	if err = replMgr.StartNetworkConnectivity(); err != nil {
		return
	}
	if err = replMgr.StartBootstrapPhase(); err != nil {
		return
	}
	return
}
