package replication

import (
	"context"
	"fmt"
	"log"
	"time"
)

const (
	LogsWithIndexNotAvailableError = "Logs with index not available error"
	EmptyWALBufferError            = "Empty WAL buffer error"
)

type (
	DataReplicationManager struct {
		ctx context.Context
		wal ReplicationWAL

		currLogIndex LogIndex
	}

	DataReplicationRequestMsg struct {
		CurrLogIndex LogIndex `json:"curr_log_index"`
		Node         *Node    `json:"node"`
	}

	DataReplicationResponseMsg struct {
		WALLogs             []WALLog `json:"wal_logs"`
		ShouldFetchSnapshot bool     `json:"should_fetch_snapshot"`
	}
)

func NewDataReplicationManager(ctx context.Context) (drMgr *DataReplicationManager, err error) {
	drMgr = &DataReplicationManager{
		ctx: ctx,
	}
	if drMgr.wal, err = NewLogTempWAL(ctx); err != nil {
		return
	}
	return
}

func (drMgr *DataReplicationManager) Persist(logs []WALLog) (err error) {
	if err = drMgr.wal.ApplyLogs(logs); err != nil {
		return
	}
	return
}

func (drMgr *DataReplicationManager) Replicate() (err error) {
	// Poll the WAL for new logs
	var (
		walLogs []WALLog
	)
	if walLogs, err = drMgr.wal.GetLogsAfterIndex("", 10); err != nil {
		return
	}
	if err = drMgr.Persist(walLogs); err != nil {
		return
	}
	return
}

func (drMgr *DataReplicationManager) pollWAL() (err error) {
	for {
		select {
		case <-drMgr.ctx.Done():
			return
		default:
			if err = drMgr.Replicate(); err != nil {
				if err == fmt.Errorf(EmptyWALBufferError) {
					time.Sleep(1 * time.Second)
					continue
				}
				log.Println("Error replicating data", err)
				continue
			}
		}
	}
	return
}

func (drMgr *DataReplicationManager) Start() (err error) {
	return
}
