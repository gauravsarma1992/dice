package replication

import (
	"context"
	"log"
	"time"
)

const (
	ReplicationManagerInContext = "replication-manager-in-context"
)

type (
	ReplicationManager struct {
		ctx        context.Context
		cancelFunc context.CancelFunc

		localNode *Node
		cluster   *Cluster

		transportMgr *TransportManager
		bootstrapMgr *BootstrapManager
		electionMgr  *ElectionManager
		hbMgr        *HeartbeatManager
		drMgr        *DataReplicationManager
		config       *ReplicationConfig
	}

	ReplicationConfig struct {
		NodeConfig              *NodeConfig
		HearbeatIntervalInSecs  int `json:"heartbeat_interval_in_secs"`
		BootstrapIntervalInSecs int `json:"bootstrap_interval_in_secs"`
	}
)

func NewReplicationManager(ctx context.Context, config *ReplicationConfig) (replMgr *ReplicationManager, err error) {
	replMgr = &ReplicationManager{
		config: config,
	}

	replMgr.ctx, replMgr.cancelFunc = context.WithCancel(ctx)
	replMgr.ctx = context.WithValue(ctx, ReplicationManagerInContext, replMgr)

	if replMgr.bootstrapMgr, err = NewBootstrapManager(replMgr.ctx); err != nil {
		return
	}
	if replMgr.hbMgr, err = NewHeartbeatManager(replMgr.ctx); err != nil {
		return
	}
	if replMgr.transportMgr, err = NewTransportManager(replMgr.ctx); err != nil {
		return
	}
	if replMgr.electionMgr, err = NewElectionManager(replMgr.ctx); err != nil {
		return
	}
	if replMgr.cluster, err = NewCluster(replMgr.ctx); err != nil {
		return
	}
	if replMgr.drMgr, err = NewDataReplicationManager(replMgr.ctx); err != nil {
		return
	}
	return
}

func (replMgr *ReplicationManager) StartBootstrapPhase() (err error) {
	if err = replMgr.bootstrapMgr.Start(); err != nil {
		return
	}
	log.Println(
		"Bootstrap phase completed. Local node - ",
		replMgr.localNode,
		"Discovered nodes -", replMgr.cluster.GetNodes(),
	)
	return
}

func (replMgr *ReplicationManager) StartHeartbeats() (err error) {
	time.Sleep(5 * time.Second)
	//log.Println("Heartbeats phase started for NodeID -", replMgr.localNode.ID)
	go replMgr.hbMgr.Start()
	return
}

func (replMgr *ReplicationManager) StartDataReplicationPhase() (err error) {
	//log.Println("Data replication phase started for NodeID -", replMgr.localNode.ID)
	if err = replMgr.drMgr.Start(); err != nil {
		return
	}
	return
}

func (replMgr *ReplicationManager) StartElectionManager() (err error) {
	//log.Println("Election manager started for NodeID -", replMgr.localNode.ID)
	if err = replMgr.electionMgr.Start(); err != nil {
		return
	}
	return
}

func (replMgr *ReplicationManager) Run() (err error) {
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
	if err = replMgr.StartElectionManager(); err != nil {
		log.Println("Error in election manager phase", err)
		return
	}
	for {
		select {
		case <-replMgr.ctx.Done():
			return
		}
	}
	log.Println("Shutting down replication manager")
	return
}
