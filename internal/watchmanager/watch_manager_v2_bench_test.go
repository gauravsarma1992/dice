package watchmanager

import (
	"strconv"
	"testing"

	"github.com/dicedb/dice/internal/cmd"
)

func createTestNodes(wm *WatchManagerV2, count int) {
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

func runBenchmarkForCardinality(b *testing.B, count int) {
	wm := GetDefaultWatchManager()
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
}

func getTestNodeVal(count int) (val string) {
	val = strconv.Itoa(count) + "arg" + strconv.Itoa(count)
	return
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
