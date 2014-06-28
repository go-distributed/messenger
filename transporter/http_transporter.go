package transporter

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"

	log "github.com/go-distributed/messenger/3rdparty/github.com/golang/glog"
)

// For internal message passing.
type message struct {
	data []byte
	err  error
}

type HTTPTransporter struct {
	hostport    string // Local address.
	messageChan chan *message
	mux         *http.ServeMux
	client      *http.Client
}

const defaultPrefix = "/messenger"
const defaultChanSize = 1024

// Create a new http transporter.
func NewHTTPTransporter(hostport string) (*HTTPTransporter, error) {
	t := &HTTPTransporter{
		hostport:    hostport,
		messageChan: make(chan *message, defaultChanSize),
		mux:         http.NewServeMux(),
		client:      new(http.Client),
	}
	t.mux.HandleFunc(defaultPrefix, t.messageHandler)
	return t, nil
}

// Send an encoded message to the host:port.
// This will block.
func (t *HTTPTransporter) Send(hostport string, b []byte) error {
	targetURL := fmt.Sprintf("http://%s%s", hostport, defaultPrefix)
	log.V(2).Infof("Sending message to %v\n", hostport)
	resp, err := t.client.Post(targetURL, "application/messenger", bytes.NewReader(b))
	if resp == nil || err != nil {
		log.Warningf("HTTPTransporter: Failed to POST: %v\n", err)
		return err
	}
	defer resp.Body.Close()
	return nil
}

// Receive a message in bytes from some peer.
func (t *HTTPTransporter) Recv() (b []byte, err error) {
	msg := <-t.messageChan
	return msg.data, msg.err
}

// Start the transporter, this will block unless some error happens.
func (t *HTTPTransporter) Start() error {
	if err := http.ListenAndServe(t.hostport, t.mux); err != nil {
		return err
	}
	return nil
}

// Stop the transporter. No-op for now.
func (t *HTTPTransporter) Stop() error {
	return nil
}

// Destroy the transporter.
func (t *HTTPTransporter) Destroy() error {
	return nil
}

// Handle incoming messages.
func (t *HTTPTransporter) messageHandler(w http.ResponseWriter, r *http.Request) {
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Warningf("HTTPTransporter: Failed to read HTTP body: %v\n", err)
	}
	log.V(2).Infof("Receiving message from %v\n", r.RemoteAddr)
	t.messageChan <- &message{b, err}
}
