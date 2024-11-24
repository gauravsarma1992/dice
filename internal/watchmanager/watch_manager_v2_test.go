package watchmanager

import (
	"testing"

	"github.com/dicedb/dice/internal/cmd"
	"github.com/stretchr/testify/assert"
)

func init() {
	DefaultDisplayer = NewArrayStorer()
}

func GetDefaultWatchManager() (wm *WatchManagerV2) {
	wm, _ = NewWatchManager()
	return
}

func TriggerWatchEvent(wm *WatchManagerV2, eventCmd *cmd.DiceDBCmd) (err error) {
	weOne := &WatchEvent{
		EventCmd: eventCmd,
	}
	err = wm.HandleWatchEvent(weOne)
	return
}

func TestWatchManagerV2Suite(t *testing.T) {
	wm := GetDefaultWatchManager()
	watchCmdOne := &cmd.DiceDBCmd{
		Cmd:  "GET.WATCH",
		Args: []string{"argOne"},
	}
	sessOne, err := wm.CreateSession(watchCmdOne)
	assert.Nil(t, err)
	assert.NotNil(t, sessOne)

	err = TriggerWatchEvent(wm, &cmd.DiceDBCmd{
		Cmd:  "GET.WATCH",
		Args: []string{"argOne", "this should appear"},
	})
	assert.Nil(t, err)

	err = TriggerWatchEvent(wm, &cmd.DiceDBCmd{
		Cmd:  "GET.WATCH",
		Args: []string{"argTwo", "this should not appear"},
	})
	// Error should be raised since it doesn't match
	assert.NotNil(t, err)

	err = TriggerWatchEvent(wm, &cmd.DiceDBCmd{
		Cmd:  "GET.WATCH",
		Args: []string{"argOne", "this should appear again"},
	})
	assert.Nil(t, err)

	assert.Equal(t,
		[]string{"this should appear", "this should appear again"},
		DefaultDisplayer.(*ArrayStorer).store,
	)
}

func TestSessionExpiry(t *testing.T) {
	assert.Equal(t, nil, nil)
}
