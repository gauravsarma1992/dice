package watchmanager

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/dicedb/dice/internal/cmd"
)

func createTestNodesV2(wm *WatchManagerV2, count int) {
	for idx := 0; idx < count; idx++ {
		watchCmdOne := &cmd.DiceDBCmd{
			Cmd: "GET.WATCH",
			// Adding idx both to the start and end to increase
			// the height and the width of the trie
			Args: []string{getTestNodeVal(idx)},
		}
		wm.CreateSession(watchCmdOne)
	}
}

func runBenchmarkForCardinalityV2(b *testing.B, count int) {
	outStr := []string{}
	ctx, cancelFunc := context.WithCancel(context.Background())

	go readInputFromChV2(ctx, DefaultDisplayer.(*ChannelSender).inpCh, &outStr)
	time.Sleep(1 * time.Second)

	wm := getDefaultWatchManagerV2()
	createTestNodesV2(wm, count)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		triggerWatchEventV2(wm, &cmd.DiceDBCmd{
			Cmd:  "GET.WATCH",
			Args: []string{getTestNodeVal(count / 2), "this should appear again"},
		})
		triggerWatchEventV2(wm, &cmd.DiceDBCmd{
			Cmd:  "GET.WATCH",
			Args: []string{getTestNodeVal(1), "this should appear again"},
		})
		triggerWatchEventV2(wm, &cmd.DiceDBCmd{
			Cmd:  "GET.WATCH",
			Args: []string{getTestNodeVal(count - 1), "this should appear again"},
		})
	}

	b.StopTimer()
	cancelFunc()

}

func getTestNodeVal(count int) (val string) {
	val = strconv.Itoa(count) + "arg" + strconv.Itoa(count)
	return
}

func BenchmarkKeysWithHighCardinalityBreadthWiseV2(b *testing.B) {
	runBenchmarkForCardinalityV2(b, 100000)
}

func BenchmarkKeysWithHighCardinalityDepthWiseV2(b *testing.B) {
	runBenchmarkForCardinalityV2(b, 1000)
}

func BenchmarkKeysWithLowCardinalityV2(b *testing.B) {
	runBenchmarkForCardinalityV2(b, 10)
}
