package perf

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"sync/atomic"

	"github.com/go-redis/redis"
)

type (
	PerfClientMgr struct {
		ctx          context.Context
		cancelFunc   context.CancelFunc
		ClientsCount uint16
		Clients      []*PerfClient
		waitGroup    *sync.WaitGroup
		RunForSecs   uint32
	}
	PerfClient struct {
		id          int
		ctx         context.Context
		startTime   time.Time
		redisClient *redis.Client

		reqsSent atomic.Uint32
	}
)

func genRandomString(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	rand.Seed(time.Now().UnixNano())
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func NewPerfClient(ctx context.Context, id int) (pc *PerfClient, err error) {
	pc = &PerfClient{
		id:  id,
		ctx: ctx,
	}

	return
}

func (pc *PerfClient) setupAndTestRedisConn() (err error) {
	// Create a new Redis client
	pc.redisClient = redis.NewClient(&redis.Options{
		Addr: "localhost:7379", // Redis server address
	})

	// Test connection
	if _, err = pc.redisClient.Ping().Result(); err != nil {
		return
	}
	return
}

func (pc *PerfClient) fireReq() (err error) {
	value := genRandomString(10)

	// Set the key in Redis
	err = pc.redisClient.Set(value, value, 0).Err()
	if err != nil {
		fmt.Println("SET error:", err)
		return
	}

	// Get the value back from Redis
	if _, err = pc.redisClient.Get(value).Result(); err != nil {
		fmt.Println("GET error:", err)
		return
	}

	pc.reqsSent.Add(1)
	return
}

func (pc *PerfClient) fireReqs() (err error) {
	for {
		select {
		case <-pc.ctx.Done():
			//fmt.Println("Completed execution for client -", pc.id, ". Total Reqs - ", pc.reqsSent.Load())
			return
		default:
			for idx := 0; idx < 1000; idx++ {
				if err = pc.fireReq(); err != nil {
					return
				}
			}
		}
	}
	return
}

func (pc *PerfClient) Run() (err error) {
	fmt.Println("Starting perf test by client", pc.id)
	if err = pc.setupAndTestRedisConn(); err != nil {
		return
	}
	defer pc.redisClient.Close()
	if err = pc.fireReqs(); err != nil {
		return
	}
	return
}

func NewPerfClientMgr() (pf *PerfClientMgr, err error) {
	pf = &PerfClientMgr{
		waitGroup:    &sync.WaitGroup{},
		ClientsCount: 5,
		RunForSecs:   50,
	}

	pf.ctx, pf.cancelFunc = context.WithCancel(context.Background())

	for idx := 0; idx < int(pf.ClientsCount); idx++ {
		var (
			pc *PerfClient
		)
		if pc, err = NewPerfClient(pf.ctx, idx); err != nil {
			return
		}
		pf.Clients = append(pf.Clients, pc)
	}
	return
}

func (pf *PerfClientMgr) CollectStats() (err error) {
	totalReqsSent := 0
	for _, pc := range pf.Clients {
		totalReqsSent += int(pc.reqsSent.Load())
	}
	fmt.Println("Total Requests sent by all the clients", totalReqsSent)
	return
}

func (pf *PerfClientMgr) Run() (err error) {
	for _, pc := range pf.Clients {
		go pc.Run()
	}
	time.Sleep(time.Second * time.Duration(pf.RunForSecs))
	fmt.Println("Completed execution, shutting down clients")
	pf.cancelFunc()
	time.Sleep(time.Second * 2)
	pf.CollectStats()

	return
}

func BenchmarkPerfGetSetCmds(b *testing.B) {
	pfMgr, err := NewPerfClientMgr()
	if err != nil {
		fmt.Println(err)
		return
	}
	for i := 0; i < b.N; i++ {
		b.ResetTimer()
		pfMgr.ClientsCount = 100
		pfMgr.Run()
	}
}
