package replication

import "context"

type (
	DataReplicationManager struct {
		ctx context.Context
	}
)

func NewDataReplicationManager(ctx context.Context) (drMgr *DataReplicationManager, err error) {
	drMgr = &DataReplicationManager{
		ctx: ctx,
	}
	return
}

func (drMgr *DataReplicationManager) Start() (err error) {
	return
}
