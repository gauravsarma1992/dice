package replication

import "context"

type (
	LogIndex string

	WALLog struct {
		Index LogIndex `json:"index"`
		Value []byte   `json:"value"`
	}

	ReplicationWAL interface {
		GetLogsAfterIndex(LogIndex, int) ([]WALLog, error)
		ApplyLogs([]WALLog) error
		GetSnapshot() ([]byte, error)
	}

	LogTempWAL struct {
		ctx     context.Context
		walLogs []WALLog
	}
)

func NewLogTempWAL(ctx context.Context) (logWal *LogTempWAL, err error) {
	logWal = &LogTempWAL{
		ctx:     ctx,
		walLogs: make([]WALLog, 0),
	}
	return
}

func (logWal *LogTempWAL) GetLogsAfterIndex(index LogIndex, limit int) (logs []WALLog, err error) {
	for _, log := range logWal.walLogs {
		if log.Index > index {
			logs = append(logs, log)
		}
		if len(logs) == limit {
			break
		}
	}
	return
}

func (logWal *LogTempWAL) ApplyLogs(logs []WALLog) (err error) {
	logWal.walLogs = append(logWal.walLogs, logs...)
	return
}

func (logWal *LogTempWAL) GetSnapshot() (snapshot []byte, err error) {
	return
}
