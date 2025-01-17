package replication

import (
	"context"
	"log"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j/db"
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
		electionMgr  *ElectionManager
		hbMgr        *HeartbeatManager
		drMgr        *db.DataReplicationManager
		config       *ReplicationConfig
	}

	ReplicationConfig struct {
		NodeConfig *NodeConfig
	}
)

func NewReplicationManager(ctx context.Context, config *ReplicationConfig) (replMgr *ReplicationManager, err error) {
	replMgr = &ReplicationManager{
		config: config,
	}
	replMgr.ctx, replMgr.cancelFunc = context.WithCancel(ctx)
	replMgr.ctx = context.WithValue(replMgr.ctx, ReplicationManagerInContext, replMgr)

	if replMgr.transportMgr, err = NewTransportManager(replMgr.ctx); err != nil {
		return
	}
	replMgr.ctx = context.WithValue(replMgr.ctx, TransportManagerInContext, replMgr.transportMgr)
	if replMgr.cluster, err = NewCluster(replMgr.ctx, replMgr.localNode); err != nil {
		return
	}
	if replMgr.hbMgr, err = NewHeartbeatManager(replMgr.ctx); err != nil {
		return
	}
	if replMgr.electionMgr, err = NewElectionManager(replMgr.ctx); err != nil {
		return
	}
	if replMgr.drMgr, err = NewDataReplicationManager(replMgr.ctx); err != nil {
		return
	}
	return
}

func (replMgr *ReplicationManager) StartNetworkConnectivity() (err error) {
	log.Println("Network connectivity established")
	return
}

func (replMgr *ReplicationManager) StartBootstrapPhase() (err error) {
	var (
		discoveredNodes []*Node
	)
	if replMgr.localNode, err = NewNode(replMgr.ctx, replMgr.config.NodeConfig); err != nil {
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
	log.Println("Bootstrap phase completed")
	return
}

func (replMgr *ReplicationManager) StartHeartbeats() (err error) {
	log.Println("Heartbeats phase started")
	go replMgr.hbMgr.Start()
	return
}

func (replMgr *ReplicationManager) StartDataReplicationPhase() (err error) {
	log.Println("Data replication phase completed")
	if err = replMgr.drMgr.Start(); err != nil {
		return
	}
	return
}

func (replMgr *ReplicationManager) StartElectionManager() (err error) {
	log.Println("Election manager started")
	if err = replMgr.electionMgr.Start(); err != nil {
		return
	}
	return
}

func (replMgr *ReplicationManager) Run() (err error) {
	if err = replMgr.StartNetworkConnectivity(); err != nil {
		log.Println("Error in setting up network connectivity", err)
		return
	}
	if err = replMgr.StartBootstrapPhase(); err != nil {
		log.Println("Error in bootstrap phase", err)
		return
	}
	if err = replMgr.StartHeartbeats(); err != nil {
		log.Println("Error in heartbeats phase", err)
		return
	}
	if err = replMgr.StartDataReplicationPhase(); err != nil {
		log.Println("Error in data replication phase", err)
		return
	}
	if err = replMgr.StartDataReplicationPhase(); err != nil {
		log.Println("Error in data replication phase", err)
		return
	}
	log.Println("Shutting down replication manager")
	return
}
