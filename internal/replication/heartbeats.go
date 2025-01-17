package replication

import "context"

type (
	HeartbeatManager struct {
		ctx context.Context
	}
)

func NewHeartbeatManager(ctx context.Context) (hbMgr *HeartbeatManager, err error) {
	hbMgr = &HeartbeatManager{
		ctx: ctx,
	}
	return
}

func (hbMgr *HeartbeatManager) Start() (err error) {
	return
}
