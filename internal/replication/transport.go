package replication

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
)

type (
	Transport interface {
		Ping(string) (*Message, error)
		Send(*Message) (*Message, error)
		Recv(*Message) (*Message, error)
	}

	HttpTransport struct {
		ctx    context.Context
		client *http.Client
		server *gin.Engine
	}
)

func NewTransport(ctx context.Context) (transport Transport, err error) {
	transport, err = NewHttpTransport(ctx)
	return
}

func NewHttpTransport(ctx context.Context) (httpTransport *HttpTransport, err error) {
	httpTransport = &HttpTransport{
		ctx:    ctx,
		client: &http.Client{},
		server: gin.Default(),
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

func (httpTransport *HttpTransport) messageHandler(c *gin.Context) {
	var (
		receivedMsg *Message
		err         error
	)
	receivedMsg = &Message{}
	if err = c.ShouldBindJSON(receivedMsg); err != nil {
		log.Println("json message could not be parsed and failed with error", err)
		return
	}
	return
}

func (httpTransport *HttpTransport) Ping(remoteIp string) (respMsg *Message, err error) {
	return nil, nil
}

func (httpTransport *HttpTransport) Send(reqMsg *Message) (respMsg *Message, err error) {
	var (
		httpReq     *http.Request
		httpResp    *http.Response
		httpReqBody []byte
	)
	if httpReqBody, err = json.Marshal(reqMsg); err != nil {
		return
	}
	httpReqBodyBuffer := bytes.NewReader(httpReqBody)
	if httpReq, err = http.NewRequest("POST", "/handler", httpReqBodyBuffer); err != nil {
		return
	}
	if httpResp, err = httpTransport.client.Do(httpReq); err != nil {
		return
	}
	respMsg = NewMessage(
		InfoMessageGroup,
		reqMsg.RemoteNodeID,
		reqMsg.LocalNodeID,
		httpResp,
	)
	return
}

func (httpTransport *HttpTransport) Recv(reqMsg *Message) (respMsg *Message, err error) {
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
