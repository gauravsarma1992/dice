package replication

type (
	LogIndex string

	Log struct {
		Index LogIndex `json:"index"`
		Value []byte   `json:"value"`
	}

	WAL interface {
		GetLogsAfterIndex(LogIndex, int) ([]Log, error)
		ApplyLogs([]Log) error
		GetSnapshot() ([]byte, error)
	}
)

func NewWAL() (wal *WAL, err error) {
	return
}
