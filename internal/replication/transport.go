package replication

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
		localNode        *Node
		client           *http.Client
		server           *gin.Engine
		transportManager *TransportManager
	}
)

func NewTransport(ctx context.Context, node *Node) (transport Transport, err error) {
	transport, err = NewHttpTransport(ctx, node)
	return
}

func NewHttpTransport(ctx context.Context, node *Node) (httpTransport *HttpTransport, err error) {
	httpTransport = &HttpTransport{
		ctx:              ctx,
		localNode:        node,
		client:           &http.Client{},
		server:           gin.Default(),
		transportManager: ctx.Value(TransportManagerInContext).(*TransportManager),
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

func (httpTransport *HttpTransport) getRemoteUrl(remoteIp string) string {
	return fmt.Sprintf("http://%s:%s", remoteIp, httpTransport.localNode.config.RemotePort)
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
		httpResp    *http.Response
		httpReqBody []byte
	)
	type (
		HttpRespMessage struct {
			Message *Message `json:"message"`
		}
	)
	if httpReqBody, err = json.Marshal(reqMsg); err != nil {
		return
	}
	httpReqBodyBuffer := bytes.NewReader(httpReqBody)
	if httpReq, err = http.NewRequest(
		"POST",
		fmt.Sprintf("%s/handler", httpTransport.getRemoteUrl(remoteIp)),
		httpReqBodyBuffer,
	); err != nil {
		return
	}
	if httpResp, err = httpTransport.client.Do(httpReq); err != nil {
		return
	}
	defer httpResp.Body.Close()

	httpRespMsg := &HttpRespMessage{}
	httpRespBodyB, _ := io.ReadAll(httpResp.Body)

	if err = json.Unmarshal(httpRespBodyB, httpRespMsg); err != nil {
		log.Println("Error decoding JSON response:", err)
		return
	}

	respMsg = httpRespMsg.Message
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
	log.Println("Received message: ", receivedMsg)
	if msgHandler, isPresent = httpTransport.transportManager.msgHandlers[receivedMsg.Type]; !isPresent {
		err = errors.New("Message handler not set")
		log.Println("failed to fetch message handler", err)
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
		if err = httpTransport.server.Run(
			fmt.Sprintf("%s:%s",
				httpTransport.localNode.config.LocalHost,
				httpTransport.localNode.config.LocalPort,
			),
		); err != nil {
			log.Println("http transport failed")
		}
	}()
	return
}
