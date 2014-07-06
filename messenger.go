package messenger

import (
	"fmt"
	"reflect"
	"time"

	log "github.com/go-distributed/messenger/3rdparty/github.com/golang/glog"
	"github.com/go-distributed/messenger/codec"
	"github.com/go-distributed/messenger/transporter"
)

const defaultQueueSize = 1024
const preparePeriod = time.Second * 1

type MessageHandler func(interface{})

type messageToSend struct {
	hostport string
	msg      interface{}
}

// A messenger can send to a specified peer
// or receive messages from peers.
type Messenger struct {
	codec     codec.Codec
	tr        transporter.Transporter
	inQueue   chan interface{}    // For incomming messages.
	outQueue  chan *messageToSend // For outgoing messages.
	recvQueue chan interface{}    // Buffer for recv messages.

	handlers           map[reflect.Type]MessageHandler
	registeredMessages map[reflect.Type]bool
	stop               chan struct{}
	enableRecv         bool
	enableHandler      bool
}

// Create a new messenger.
// If enableRecv is set to true, then the user should be
// responsible to consume the message via Recv(), otherwise
// the the underlying reading will stop if the queue is full.
// At least one of the enableRecv and enableHandler should be
// set to true.
func New(codec codec.Codec, tr transporter.Transporter,
	enableRecv, enableHandler bool) *Messenger {
	if !enableRecv && !enableHandler {
		log.Warningf("Neither recv or handler is enabled\n")
		return nil
	}
	return &Messenger{
		codec:              codec,
		tr:                 tr,
		inQueue:            make(chan interface{}, defaultQueueSize),
		outQueue:           make(chan *messageToSend, defaultQueueSize),
		recvQueue:          make(chan interface{}, defaultQueueSize),
		handlers:           make(map[reflect.Type]MessageHandler),
		registeredMessages: make(map[reflect.Type]bool),
		stop:               make(chan struct{}),
		enableRecv:         enableRecv,
		enableHandler:      enableHandler,
	}
}

// Register a message in the messenger.
// It will call the undelying codec to register the message as well.
func (m *Messenger) RegisterMessage(msg interface{}) error {
	msgType := reflect.TypeOf(msg)
	if _, ok := m.registeredMessages[msgType]; ok {
		return fmt.Errorf("Message type %v already registered", msgType)
	}
	if err := m.codec.RegisterMessage(msg); err != nil {
		return err
	}
	m.registeredMessages[msgType] = true
	return nil
}

// Register a message with a handler.
// When such a message comes in, it will be passed to
// the handler.
func (m *Messenger) RegisterHandler(msg interface{}, msgHandler MessageHandler) error {
	if !m.enableHandler {
		return fmt.Errorf("Cannot register handler since it's disabled")
	}

	msgType := reflect.TypeOf(msg)
	if _, ok := m.handlers[msgType]; ok {
		return fmt.Errorf("Message type: %v is already registered", msgType)
	}
	m.handlers[msgType] = msgHandler
	return nil
}

// Start the messenger.
func (m *Messenger) Start() error {
	if err := m.codec.Initial(); err != nil {
		return err
	}

	errChan := make(chan error)
	go func() {
		if err := m.tr.Start(); err != nil {
			errChan <- err
		}
	}()

	select {
	case err := <-errChan:
		return err
	case <-time.After(preparePeriod):
	}

	go m.incomingLoop()
	go m.outgoingLoop()
	go m.readingLoop()
	return nil
}

// From the wire to the queue.
func (m *Messenger) incomingLoop() {
	for {
		select {
		case <-m.stop:
			return
		default:
		}

		b, err := m.tr.Recv()
		if err != nil {
			log.Warningf("Transporter Recv() error: %v\n", err)
			continue
		}
		msg, err := m.codec.Unmarshal(b)
		if err != nil {
			log.Warningf("Codec Unmarshal() error: %v\n", err)
			continue
		}
		m.inQueue <- msg
	}
}

// From the queue to callbacks / recvQueue.
func (m *Messenger) readingLoop() {
	for {
		select {
		case <-m.stop:
			return
		case msg := <-m.inQueue:
			msgType := reflect.TypeOf(msg)
			// Verify message type.
			if _, ok := m.registeredMessages[msgType]; !ok {
				log.Warningf("Unregistered message type: %v\n", msgType)
				continue
			}
			// Pass the message to the handler.
			if m.enableHandler {
				if h, ok := m.handlers[msgType]; ok {
					h(msg)
				}
			}
			// Pass the message to the receive queue.
			if m.enableRecv {
				m.recvQueue <- msg
			}
		}
	}
}

// From the queue to the wire.
func (m *Messenger) outgoingLoop() {
	for {
		select {
		case <-m.stop:
			return
		case mts := <-m.outQueue:
			// TODO: Verify message type.
			b, err := m.codec.Marshal(mts.msg)
			if err != nil {
				log.Warningf("Codec Marshal() error: %v\n", err)
				continue
			}

			if err = m.tr.Send(mts.hostport, b); err != nil {
				log.Warningf("Transporter Send() error: %v\n", err)
				continue
			}
		}
	}
}

// Stop the messenger.
func (m *Messenger) Stop() error {
	close(m.stop)
	return m.tr.Stop()
}

// Send a message.
func (m *Messenger) Send(hostport string, msg interface{}) error {
	// Verify the message.
	msgType := reflect.TypeOf(msg)
	if _, ok := m.registeredMessages[msgType]; !ok {
		return fmt.Errorf("Unregistered message type: %v\n", msgType)
	}

	m.outQueue <- &messageToSend{hostport, msg}
	return nil
}

// Recv a message.
func (m *Messenger) Recv() (interface{}, error) {
	msg, ok := <-m.recvQueue
	if !ok {
		return nil, fmt.Errorf("Failed to receive, channel closed\n")
	}
	return msg, nil
}

// Destroy the messenger.
func (m *Messenger) Destroy() error {
	select {
	case <-m.stop:
	default:
		// Stop the messenger if not already.
		if err := m.Stop(); err != nil {
			return err
		}
	}
	if err := m.codec.Destroy(); err != nil {
		return err
	}
	if err := m.tr.Destroy(); err != nil {
		return err
	}
	return nil
}
