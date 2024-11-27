package watchmanager

import (
	"context"
	"fmt"
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

func createTestNodes(wm *WatchManager, count int) {
	for idx := 0; idx < count; idx++ {
		subs := WatchSubscription{
			Subscribe:    true,
			AdhocReqChan: make(chan *cmd.DiceDBCmd),
			WatchCmd: &cmd.DiceDBCmd{
				Cmd:  "GET.WATCH",
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

func readInputFromCh(ctx context.Context, inpCh chan string, outStr *[]string) {
	for {
		select {
		case res := <-inpCh:
			fmt.Println(res)
			*outStr = append(*outStr, res)
		case <-ctx.Done():
			return
		}
	}
	return
}

func runBenchmarkForCardinality(b *testing.B, count int) {
	outStr := []string{}
	ctx, cancelFunc := context.WithCancel(context.Background())

	go readInputFromCh(ctx, DefaultDisplayer.(*ChannelSender).inpCh, &outStr)
	time.Sleep(1 * time.Second)

	wm := getDefaultWatchManager()
	createTestNodes(wm, count)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		triggerWatchEvent(wm, &cmd.DiceDBCmd{
			Cmd:  "GET.WATCH",
			Args: []string{getTestNodeVal(count / 2), "this should appear again"},
		})
		triggerWatchEvent(wm, &cmd.DiceDBCmd{
			Cmd:  "GET.WATCH",
			Args: []string{getTestNodeVal(1), "this should appear again"},
		})
		triggerWatchEvent(wm, &cmd.DiceDBCmd{
			Cmd:  "GET.WATCH",
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
