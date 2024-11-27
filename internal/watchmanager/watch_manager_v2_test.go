package watchmanager

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/dicedb/dice/internal/cmd"
	"github.com/stretchr/testify/assert"
)

func getDefaultWatchManagerV2() (wm *WatchManagerV2) {
	wm, _ = NewWatchManagerV2()
	return
}

func triggerWatchEventV2(wm *WatchManagerV2, eventCmd *cmd.DiceDBCmd) (err error) {
	weOne := &WatchEvent{
		EventCmd: eventCmd,
	}
	err = wm.HandleWatchEvent(weOne)
	if err != nil {
		log.Println(err)
	}
	return
}

func readInputFromChV2(ctx context.Context, inpCh chan string, outStr *[]string) {
	for {
		select {
		case res := <-inpCh:
			*outStr = append(*outStr, res)
		case <-ctx.Done():
			return
		}
	}
	return
}

func TestWatchManagerV2Suite(t *testing.T) {
	outStr := []string{}
	ctx, cancelFunc := context.WithCancel(context.Background())

	go readInputFromChV2(ctx, DefaultDisplayer.(*ChannelSender).inpCh, &outStr)
	time.Sleep(1 * time.Second)

	wm := getDefaultWatchManagerV2()
	watchCmdOne := &cmd.DiceDBCmd{
		Cmd:  "GET.WATCH",
		Args: []string{"argOne"},
	}
	sessOne, err := wm.CreateSession(watchCmdOne)
	assert.Nil(t, err)
	assert.NotNil(t, sessOne)

	err = triggerWatchEventV2(wm, &cmd.DiceDBCmd{
		Cmd:  "GET.WATCH",
		Args: []string{"argOne", "this should appear"},
	})
	assert.Nil(t, err)

	err = triggerWatchEventV2(wm, &cmd.DiceDBCmd{
		Cmd:  "GET.WATCH",
		Args: []string{"argTwo", "this should not appear"},
	})
	// Error should be raised since it doesn't match
	assert.NotNil(t, err)

	err = triggerWatchEventV2(wm, &cmd.DiceDBCmd{
		Cmd:  "GET.WATCH",
		Args: []string{"argOne", "this should appear again"},
	})
	assert.Nil(t, err)
	cancelFunc()
	time.Sleep(1 * time.Second)

	assert.Equal(t,
		[]string{"this should appear", "this should appear again"},
		outStr,
	)
}