package replication

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
)

type (
	Transport interface {
		Ping(string) (*Message, error)
		Send(*Message) (*Message, error)
	}

	HttpTransport struct {
		ctx              context.Context
		client           *http.Client
		server           *gin.Engine
		transportManager *TransportManager
		localNode        *Node
	}
)

func NewTransport(ctx context.Context) (transport Transport, err error) {
	transport, err = NewHttpTransport(ctx)
	return
}

func NewHttpTransport(ctx context.Context) (httpTransport *HttpTransport, err error) {
	httpTransport = &HttpTransport{
		ctx:              ctx,
		client:           &http.Client{},
		server:           gin.Default(),
		transportManager: ctx.Value(TransportManagerInContext).(*TransportManager),
		localNode:        ctx.Value(LocalNodeInContext).(*Node),
	}
	if err = httpTransport.setup(); err != nil {
		return
	}
	if err = httpTransport.run(); err != nil {
		return
	}
	return
}

func (httpTransport *HttpTransport) setup() (err error) {
	httpTransport.server.POST("/handler", httpTransport.messageHandler)
	return
}

func (httpTransport *HttpTransport) Ping(remoteIp string) (respMsg *Message, err error) {
	var (
		reqMsg *Message
	)
	reqMsg = NewMessage(
		InfoMessageGroup,
		PingMessageType,
		httpTransport.localNode.ID,
		0,
		&PingRequest{Node: httpTransport.localNode},
	)
	if respMsg, err = httpTransport.send(remoteIp, reqMsg); err != nil {
		return
	}
	return
}

func (httpTransport *HttpTransport) Send(reqMsg *Message) (respMsg *Message, err error) {
	var (
		remoteIp     string
		remoteNodeID NodeID
	)
	if remoteIp, err = httpTransport.transportManager.ConvertNodeToIp(remoteNodeID); err != nil {
		return
	}
	if respMsg, err = httpTransport.send(remoteIp, reqMsg); err != nil {
		return
	}
	return
}

func (httpTransport *HttpTransport) send(remoteIp string, reqMsg *Message) (respMsg *Message, err error) {
	var (
		httpReq     *http.Request
		httpReqBody []byte
	)
	if httpReqBody, err = json.Marshal(reqMsg); err != nil {
		return
	}
	httpReqBodyBuffer := bytes.NewReader(httpReqBody)
	if httpReq, err = http.NewRequest("POST", "/handler", httpReqBodyBuffer); err != nil {
		return
	}
	if _, err = httpTransport.client.Do(httpReq); err != nil {
		return
	}
	return
}

func (httpTransport *HttpTransport) messageHandler(c *gin.Context) {
	var (
		receivedMsg  *Message
		generatedMsg *Message
		msgHandler   MessageHandler
		isPresent    bool
		err          error
	)
	receivedMsg = &Message{}
	if err = c.ShouldBindJSON(receivedMsg); err != nil {
		log.Println("json message could not be parsed and failed with error", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if msgHandler, isPresent = httpTransport.transportManager.msgHandlers[receivedMsg.Type]; !isPresent {
		err = errors.New("Message handler not set")
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if generatedMsg, err = msgHandler(receivedMsg); err != nil {
		log.Println("message handler failed with error", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"message": generatedMsg,
	})
	return
}

func (httpTransport *HttpTransport) run() (err error) {
	go func() {
		if err = httpTransport.server.Run(); err != nil {
			log.Println("http transport failed")
		}
	}()
	return
}
