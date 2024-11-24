package watchmanager

import (
	"fmt"
	"log"

	"github.com/dicedb/dice/internal/cmd"
)

const (
	InitiatedSessionStatus = uint8(0)
	ConnectedSessionStatus
	HungSessionStatus
	TerminatedSessionStatus
)

type (
	WatchManagerV2 struct {
		sessions []*WatchSession
		root     *WatchNode
	}

	WatchSession struct {
		// WatchSession is the object which stores the Watch
		// instance being used to listen to events.
		// Each WATCH command opens a WatchSession with the
		// required filters.

		// TODO implement this
		ID        uint32
		Cmd       *cmd.DiceDBCmd
		wm        *WatchManagerV2
		nodes     []*WatchNode
		displayer Displayer

		Status uint8
	}

	WatchNode struct {
		// WatchNode refers to the representation of the user
		// provided input while running a Watch command. All
		// inputs are converted into a WatchNode(s) and represented
		// as a trie.
		// For example: if user provides "GET.WATCH fin1" as
		// an input, then the entire command is stored as multiple WatchNode
		// instances as "G -> E -> T -> . -> W -> A -> T -> C -> H -> F -> I -> N -> 1"
		Value    int
		RefCount int

		// Each WatchNode should be able to extend the chain further by defining
		// more child watch nodes
		Parent   *WatchNode
		Children []*WatchNode

		// All matching sessions are stored in the Sessions slice. Whenever an event ends
		// at a WatchNode, the event should be sent to all the Sessions at that specific node.
		Sessions []*WatchSession
	}

	WatchEvent struct {
		// WatchEvent is the event of matching the received updates
		// happening on the DB with the provided input for Watch command.
		// Technically, the WatchEvent traverses the trie and attempts
		// to find a WatchNode with a perfect match.
		// If a perfect match is found, then the Sessions on the WatchNode
		// are sent the matching WatchEvent.

		// Matching the WatchEvent with a particular level of the trie.
		// Option 1 - Matching character by character at all levels
		EventCmd        *cmd.DiceDBCmd
		Value           string
		matchedSessions []*WatchSession
	}
)

var (
	DefaultDisplayer Displayer = NewChannelSender()
)

func NewWatchManager() (wm *WatchManagerV2, err error) {
	wm = &WatchManagerV2{}
	if wm.root, err = wm.createRootNode(); err != nil {
		return
	}
	return
}

func (wm *WatchManagerV2) createRootNode() (node *WatchNode, err error) {
	node = &WatchNode{
		Value: 0, // the slash doesn't have any significance
	}
	return
}

// =========================== Session =============================
func (wm *WatchManagerV2) CreateSession(cmd *cmd.DiceDBCmd) (session *WatchSession, err error) {
	session = &WatchSession{
		wm:        wm,
		Cmd:       cmd,
		displayer: DefaultDisplayer,
		Status:    InitiatedSessionStatus,
	}
	wm.sessions = append(wm.sessions, session)
	if err = session.CreateTree(); err != nil {
		return
	}
	session.Status = ConnectedSessionStatus
	return
}

// TODO: implement this
func (wm *WatchManagerV2) DeleteSession(session *WatchSession) (err error) {
	if err = session.Close(); err != nil {
		return
	}
	return
}

func (session *WatchSession) Close() (err error) {
	session.Status = TerminatedSessionStatus
	if err = session.displayer.Close(); err != nil {
		return
	}
	if err = session.removeNodeRefs(); err != nil {
		return
	}
	return
}

// TODO: implement this
func (session *WatchSession) removeNodeRefs() (err error) {
	return
}

func (session *WatchSession) CreateTree() (err error) {
	var (
		prevNode *WatchNode
	)
	prevNode = session.wm.root
	for _, nodeChr := range session.Cmd.Repr() {
		var (
			node *WatchNode
		)
		if node, err = session.CreateOrGetNode(prevNode, int(nodeChr)); err != nil {
			return
		}
		prevNode = node
		session.nodes = append(session.nodes, node)
	}
	// End of all the nodes in the session
	prevNode.Sessions = append(prevNode.Sessions, session)
	return
}

func (session *WatchSession) CreateOrGetNode(parentNode *WatchNode, val int) (node *WatchNode, err error) {
	var (
		nodeCreated bool
	)
	if node, nodeCreated = parentNode.hasChildWithValue(val); nodeCreated {
		node.RefCount += 1
		return
	}
	node = &WatchNode{
		Parent:   parentNode,
		Value:    val,
		RefCount: 1,
	}
	parentNode.Children = append(parentNode.Children, node)
	return
}

func (session *WatchSession) send(val string) (err error) {
	if err = session.displayer.Display(val); err != nil {
		return
	}
	return
}

func (node *WatchNode) hasChildWithValue(val int) (matchingNode *WatchNode, isPresent bool) {
	for _, child := range node.Children {
		if child.Value == val {
			matchingNode = child
			isPresent = true
		}
	}
	return
}

func (wm *WatchManagerV2) getEventMatchingNode(eventCmd *cmd.DiceDBCmd) (node *WatchNode, err error) {
	var (
		prevNode *WatchNode
	)
	prevNode = wm.root
	for _, nodeChr := range eventCmd.GetReprWithoutValue() {
		var (
			matchingNode *WatchNode
			isPresent    bool
		)
		if matchingNode, isPresent = prevNode.hasChildWithValue(int(nodeChr)); !isPresent {
			err = fmt.Errorf("no matching nodes for value %s", string(nodeChr))
			return
		}
		prevNode = matchingNode
	}
	// This means that we have reached the leaf node and the matching
	// sessions can be used to return back the value
	node = prevNode
	return
}

func (wm *WatchManagerV2) getEventMatchingSessions(event *WatchEvent) (sessions []*WatchSession, err error) {
	var (
		matchingNode *WatchNode
	)
	if matchingNode, err = wm.getEventMatchingNode(event.EventCmd); err != nil {
		return
	}
	sessions = matchingNode.Sessions
	return
}

func (wm *WatchManagerV2) HandleWatchEvent(event *WatchEvent) (err error) {
	var (
		sessions []*WatchSession
	)

	if sessions, err = wm.getEventMatchingSessions(event); err != nil {
		return
	}
	for _, session := range sessions {
		if session.Status != ConnectedSessionStatus {
			log.Println("Skipping sending to session because it's in status", session.Status)
			continue
		}
		if err = session.send(event.EventCmd.GetValue()); err != nil {
			log.Println("error in sending event to session with ID", session.ID, "with err", err)
			continue
		}
	}
	return
}
