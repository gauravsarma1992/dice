package watchmanager

import (
	"context"
	"testing"
	"time"

	"github.com/dicedb/dice/internal/cmd"
	dstore "github.com/dicedb/dice/internal/store"
)

func getDefaultWatchManager() (wm *WatchManager) {
	subsChan := make(chan WatchSubscription)
	cmdWatchChan := make(chan dstore.CmdWatchEvent)

	wm = NewManager(subsChan, cmdWatchChan)

	return
}

func createTestNodes(wm *WatchManager, cliCh chan *cmd.DiceDBCmd, count int) {
	for idx := 0; idx < count; idx++ {
		subs := WatchSubscription{
			Subscribe:    true,
			AdhocReqChan: cliCh,
			WatchCmd: &cmd.DiceDBCmd{
				Cmd:  "GET",
				Args: []string{getTestNodeVal(idx)},
			},
			Fingerprint: 0,
		}
		wm.handleSubscription(subs)
	}
}

func triggerWatchEvent(wm *WatchManager, eventCmd *cmd.DiceDBCmd) (err error) {
	weOne := dstore.CmdWatchEvent{
		Cmd:         dstore.Set,
		AffectedKey: eventCmd.Args[0],
	}
	wm.handleWatchEvent(weOne)
	return
}

func readInputFromCh(ctx context.Context, inpCh chan *cmd.DiceDBCmd, outStr *[]string) {
	for {
		select {
		case <-inpCh:
			*outStr = append(*outStr, "some str")
		case <-ctx.Done():
			return
		}
	}
	return
}

func runBenchmarkForCardinality(b *testing.B, count int) {
	outStr := []string{}
	ctx, cancelFunc := context.WithCancel(context.Background())

	cliCh := make(chan *cmd.DiceDBCmd, 1000)

	go readInputFromCh(ctx, cliCh, &outStr)
	time.Sleep(1 * time.Second)

	wm := getDefaultWatchManager()
	createTestNodes(wm, cliCh, count)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		triggerWatchEvent(wm, &cmd.DiceDBCmd{
			Cmd:  "SET.WATCH",
			Args: []string{getTestNodeVal(count / 2), "this should appear again"},
		})
		triggerWatchEvent(wm, &cmd.DiceDBCmd{
			Cmd:  "SET.WATCH",
			Args: []string{getTestNodeVal(1), "this should appear again"},
		})
		triggerWatchEvent(wm, &cmd.DiceDBCmd{
			Cmd:  "SET.WATCH",
			Args: []string{getTestNodeVal(count - 1), "this should appear again"},
		})
	}

	b.StopTimer()
	cancelFunc()

}

func BenchmarkKeysWithHighCardinalityBreadthWise(b *testing.B) {
	runBenchmarkForCardinality(b, 100000)
}

func BenchmarkKeysWithHighCardinalityDepthWise(b *testing.B) {
	runBenchmarkForCardinality(b, 1000)
}

func BenchmarkKeysWithLowCardinality(b *testing.B) {
	runBenchmarkForCardinality(b, 10)
}
