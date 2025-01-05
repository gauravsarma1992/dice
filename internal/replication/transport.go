package replication

import (
	"context"
	"net/http"
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
	}
)

func NewTransport(ctx context.Context) (transport Transport, err error) {
	transport = &HttpTransport{
		ctx:    ctx,
		client: &http.Client{},
	}
	return
}

func (httpTransport *HttpTransport) Ping(remoteIp string) (respMsg *Message, err error) {
	return nil, nil
}

func (httpTransport *HttpTransport) Send(reqMsg *Message) (respMsg *Message, err error) {
	return
}

func (httpTransport *HttpTransport) Recv(reqMsg *Message) (respMsg *Message, err error) {
	return
}
