package codec

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"reflect"

	"github.com/go-distributed/messenger/3rdparty/code.google.com/p/gogoprotobuf/proto"
	log "github.com/go-distributed/messenger/3rdparty/github.com/golang/glog"
)

// Only support 256 different kinds of messages for now.
type messageType uint8

// The gogoprotobuf codec.
type GoGoProtobufCodec struct {
	registeredMessages map[reflect.Type]messageType
	reversedMap        map[messageType]reflect.Type
}

// Create a new gogpprotobuf codec.
func NewGoGoProtobufCodec() (*GoGoProtobufCodec, error) {
	g := &GoGoProtobufCodec{
		registeredMessages: make(map[reflect.Type]messageType),
		reversedMap:        make(map[messageType]reflect.Type),
	}
	return g, nil
}

// Initial the gogoprotobuf codec (no-op for now).
func (c *GoGoProtobufCodec) Initial() error {
	return nil
}

// Stop the gogoprotobuf codec (no-op for now).
func (c *GoGoProtobufCodec) Stop() error {
	return nil
}

// Destroy the gogoprotobuf codec (no-op for now).
func (c *GoGoProtobufCodec) Destroy() error {
	return nil
}

// Register a message type.
func (c *GoGoProtobufCodec) RegisterMessage(msg interface{}) error {
	msgType := reflect.TypeOf(msg)
	if _, ok := c.registeredMessages[msgType]; ok {
		return fmt.Errorf("Message type %v is already registered", msgType)
	}
	if _, ok := msg.(proto.Message); !ok {
		return fmt.Errorf("Not a protobuf message %v", msgType)
	}
	c.registeredMessages[msgType] = messageType(len(c.registeredMessages))
	return nil
}

// Marshal a message into a byte slice.
func (c *GoGoProtobufCodec) Marshal(msg interface{}) ([]byte, error) {
	var err error
	defer func() {
		if err != nil {
			log.Warningf("GoGoProtobufCodec: Failed to marshal: %v\n", err)
		}
	}()

	mtype, ok := c.registeredMessages[reflect.TypeOf(msg)]
	if !ok {
		return nil, fmt.Errorf("Unknown message type: %v", reflect.TypeOf(msg))
	}
	b, err := proto.Marshal(msg.(proto.Message))
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	if err = binary.Write(buf, binary.LittleEndian, mtype); err != nil {
		return nil, err
	}

	n, err := buf.Write(b)
	if err != nil || n != len(b) {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Unmarshal a message from a byte slice.
func (c *GoGoProtobufCodec) Unmarshal(data []byte) (interface{}, error) {
	var err error
	defer func() {
		if err != nil {
			log.Warningf("GoGoProtobufCodec: Failed to unmarshal: %v\n", err)
		}
	}()

	buf := bytes.NewBuffer(data)
	var mtype messageType
	if err = binary.Read(buf, binary.LittleEndian, &mtype); err != nil {
		return nil, err
	}

	msg := reflect.New(c.reversedMap[mtype]).Interface().(proto.Message)
	if err = proto.Unmarshal(buf.Bytes(), msg); err != nil {
		return nil, err
	}
	return msg, nil
}
