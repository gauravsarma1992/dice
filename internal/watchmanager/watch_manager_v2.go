package watchmanager

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/dicedb/dice/internal/cmd"
)

const (
	InitiatedSessionStatus = uint8(0)
	ConnectedSessionStatus
	HungSessionStatus
	TerminatedSessionStatus

	SessionHungDurationThresholdInSecs = 120
)

type (
	WatchManagerV2 struct {
		sessions []*WatchSession
		root     *WatchNode
		sLock    *sync.RWMutex
	}

	WatchSession struct {
		// WatchSession is the object which stores the Watch
		// instance being used to listen to events.
		// Each WATCH command opens a WatchSession with the
		// required filters.

		ID        int64
		Cmd       *cmd.DiceDBCmd
		wm        *WatchManagerV2
		nodes     []*WatchNode
		displayer Displayer

		Status uint8

		lastAccessedAt time.Time
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

		nLock *sync.RWMutex
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
	wm = &WatchManagerV2{
		sLock: &sync.RWMutex{},
	}
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
		ID:        time.Now().UnixNano(),
		wm:        wm,
		Cmd:       cmd,
		displayer: DefaultDisplayer,
		Status:    InitiatedSessionStatus,
	}

	if err = session.CreateTree(); err != nil {
		return
	}
	session.Status = ConnectedSessionStatus

	wm.sLock.Lock()
	defer wm.sLock.Unlock()
	wm.sessions = append(wm.sessions, session)

	return
}

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
	for _, node := range session.nodes {
		if node.removeSession(session); err != nil {
			return
		}
		if node.RefCount != 0 {
			return
		}
	}
	return
}

func (session *WatchSession) IsActive() (isActive bool) {
	if time.Now().UTC().Sub(session.lastAccessedAt).Seconds() > SessionHungDurationThresholdInSecs {
		session.Status = HungSessionStatus
		isActive = false
	}
	isActive = true
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
		nLock:    &sync.RWMutex{},
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

func (node *WatchNode) removeSession(session *WatchSession) (err error) {
	matchingIdx := -1

	node.nLock.Lock()
	defer node.nLock.Unlock()

	node.RefCount -= 1
	for idx, availableSession := range node.Sessions {
		if availableSession.ID == session.ID {
			matchingIdx = idx
			break
		}
	}
	node.Sessions = append(node.Sessions[:matchingIdx], node.Sessions[matchingIdx+1:]...)
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
	matchingNode.nLock.RLock()
	defer matchingNode.nLock.RUnlock()

	sessions = matchingNode.Sessions
	return
}

func (wm *WatchManagerV2) HandleWatchEvent(event *WatchEvent) (err error) {
	var (
		sessions []*WatchSession
	)
	wm.sLock.RLock()
	defer wm.sLock.RUnlock()

	if sessions, err = wm.getEventMatchingSessions(event); err != nil {
		return
	}
	for _, session := range sessions {
		session.lastAccessedAt = time.Now().UTC()
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

func (wm *WatchManagerV2) RemoveUnusedSessions() (removedCount int, err error) {
	var (
		activeSessions []*WatchSession
	)
	wm.sLock.Lock()
	defer wm.sLock.Unlock()

	for _, session := range wm.sessions {
		if session.Status == TerminatedSessionStatus || !session.IsActive() {
			if err = wm.DeleteSession(session); err != nil {
				log.Println("error in deleting session", session)
				continue
			}
		}
		activeSessions = append(activeSessions, session)
	}
	wm.sessions = activeSessions
	return
}

func (wm *WatchManagerV2) Run() (err error) {
	wm.RemoveUnusedSessions()
	return
}
