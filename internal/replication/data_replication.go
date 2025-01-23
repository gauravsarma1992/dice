package replication

import (
	"context"
	"fmt"
	"log"
	"time"
)

const (
	// Errors
	LogsWithIndexNotAvailableError = "Logs with index not available error"
	RemoteIndexNotMatchingError    = "Remote index not matching error"
	EmptyWALBufferError            = "Empty WAL buffer error"
	WriteConcernNotSatisfiedError  = "Write concern not satisfied error"

	// WriteConcerns
	WriteConcernMajority WriteConcernT = WriteConcernT(0)
	WriteConcernOne      WriteConcernT = WriteConcernT(1)
	WriteConcernNone     WriteConcernT = WriteConcernT(2)
)

type (
	WriteConcernT          uint8
	DataReplicationManager struct {
		ctx context.Context
		wal ReplicationWAL

		currLogIndex LogIndex

		replMgr *ReplicationManager

		writeConcern WriteConcernT
	}

	DataReplicationPushRequestMsg struct {
		CurrLogIndex LogIndex `json:"curr_log_index"`
		WALLogs      []WALLog `json:"wal_logs"`
	}

	DataReplicationPushResponseMsg struct {
		CurrLogIndex LogIndex `json:"curr_log_index"`
	}
)

func NewDataReplicationManager(ctx context.Context, wal ReplicationWAL) (drMgr *DataReplicationManager, err error) {
	drMgr = &DataReplicationManager{
		ctx:          ctx,
		wal:          wal,
		writeConcern: WriteConcernNone,
	}
	drMgr.replMgr = ctx.Value(ReplicationManagerInContext).(*ReplicationManager)
	return
}

func (drMgr *DataReplicationManager) PersistLocally(logs []WALLog) (err error) {
	if err = drMgr.wal.ApplyLogs(logs); err != nil {
		return
	}
	return
}

func (drMgr *DataReplicationManager) getCurrentLogIndexFromLogs(logs []WALLog) (index LogIndex) {
	if len(logs) == 0 {
		index = LogIndex(0)
		return
	}
	index = logs[len(logs)-1].Index
	return
}

func (drMgr *DataReplicationManager) DataReplicationPushHandler(reqMsg *Message) (respMsg *Message, err error) {
	var (
		dataReplicationPushReq *DataReplicationPushRequestMsg
	)
	dataReplicationPushReq = &DataReplicationPushRequestMsg{}
	// Receive the wal logs
	if err = reqMsg.FillValue(dataReplicationPushReq); err != nil {
		return
	}
	// Persist the wal logs locally
	if err = drMgr.PersistLocally(dataReplicationPushReq.WALLogs); err != nil {
		return
	}
	log.Println("Received data from remote node", len(dataReplicationPushReq.WALLogs), dataReplicationPushReq.CurrLogIndex, reqMsg.Local)
	// Send the current log index back to the remote node
	respMsg = NewMessage(
		InfoMessageGroup,
		DataReplicationPushMessageType,
		drMgr.replMgr.localNode.GetLocalUser(),
		reqMsg.Local,
		&DataReplicationPushResponseMsg{
			CurrLogIndex: drMgr.getCurrentLogIndexFromLogs(dataReplicationPushReq.WALLogs),
		},
	)
	return
}

func (drMgr *DataReplicationManager) replicateToNode(node *Node, walLogs []WALLog) (err error) {
	var (
		reqMsg   *Message
		respMsg  *Message
		pushResp *DataReplicationPushResponseMsg
	)
	reqMsg = NewMessage(
		InfoMessageGroup,
		DataReplicationPushMessageType,
		drMgr.replMgr.localNode.GetLocalUser(),
		node.GetLocalUser(),
		&DataReplicationPushRequestMsg{
			CurrLogIndex: drMgr.currLogIndex,
			WALLogs:      walLogs,
		},
	)
	if respMsg, err = drMgr.replMgr.transportMgr.Send(reqMsg); err != nil {
		log.Println("Error sending data to remote node", err)
		return
	}
	pushResp = &DataReplicationPushResponseMsg{}
	if err = respMsg.FillValue(pushResp); err != nil {
		return
	}
	if pushResp.CurrLogIndex != drMgr.getCurrentLogIndexFromLogs(walLogs) {
		err = fmt.Errorf(RemoteIndexNotMatchingError)
		return
	}
	return
}

func (drMgr *DataReplicationManager) handleWriteConcerns(errCount int) (err error) {
	switch drMgr.writeConcern {
	case WriteConcernNone:
		err = nil
		return
	case WriteConcernOne:
		if errCount == 1 {
			err = nil
			return
		}
	case WriteConcernMajority:
		if errCount > len(drMgr.replMgr.cluster.GetRemoteNodes())/2 {
			err = nil
			return
		}
	default:
		err = fmt.Errorf(WriteConcernNotSatisfiedError)
		return
	}

	return
}

// TODO: make this async
func (drMgr *DataReplicationManager) replicateToRemoteNodes(walLogs []WALLog) (err error) {
	var (
		errCount int
	)
	for _, node := range drMgr.replMgr.cluster.GetRemoteNodes() {
		log.Println("Replicating data to remote node", node, len(walLogs))
		if err = drMgr.replicateToNode(node, walLogs); err != nil {
			errCount += 1
			log.Println("Error replicating data to remote node", err)
			continue
		}
	}
	if err = drMgr.handleWriteConcerns(errCount); err != nil {
		return
	}
	return
}

func (drMgr *DataReplicationManager) Replicate(walLogs []WALLog) (err error) {
	if err = drMgr.replicateToRemoteNodes(walLogs); err != nil {
		return
	}
	if err = drMgr.PersistLocally(walLogs); err != nil {
		return
	}
	return
}

func (drMgr *DataReplicationManager) updateCurrLogIndex(index LogIndex) (err error) {
	drMgr.currLogIndex = index
	return
}

func (drMgr *DataReplicationManager) pollLocalWAL() (err error) {
	var (
		walLogs []WALLog
	)
	if walLogs, err = drMgr.wal.GetLogsAfterIndex(drMgr.currLogIndex, 10); err != nil {
		return
	}
	if len(walLogs) == 0 {
		err = fmt.Errorf(EmptyWALBufferError)
		return
	}
	log.Println("Polling local wal data", len(walLogs))
	if err = drMgr.Replicate(walLogs); err != nil {
		log.Println("Error replicating data", err)
		return
	}
	if err = drMgr.updateCurrLogIndex(drMgr.getCurrentLogIndexFromLogs(walLogs)); err != nil {
		return
	}
	return
}

func (drMgr *DataReplicationManager) Start() (err error) {
	for {
		select {
		case <-drMgr.ctx.Done():
			return
		default:
			if drMgr.replMgr.localNode.NodeType != NodeTypeLeader {
				time.Sleep(5 * time.Second)
				continue
			}
			if err = drMgr.pollLocalWAL(); err != nil {
				time.Sleep(5 * time.Second)
				continue
			}
		}
	}
	return
}
