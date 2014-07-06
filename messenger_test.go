package messenger

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/go-distributed/messenger/3rdparty/code.google.com/p/gogoprotobuf/proto"
	"github.com/go-distributed/messenger/3rdparty/github.com/testify/assert"
	"github.com/go-distributed/messenger/codec"
	example "github.com/go-distributed/messenger/codec/testexample"
	"github.com/go-distributed/messenger/transporter"
)

var count1 int
var count2 int
var count3 int
var count4 int

// Simple message handlers used for testing.
func handler1(msg interface{}) {
	count1++
}

func handler2(msg interface{}) {
	count2++
}

func handler3(msg interface{}) {
	count3++
}

func handler4(msg interface{}) {
	count4++
}

// A simple echo server used for testing.
type echoServer struct {
	m        *Messenger
	peerAddr string
}

func (e *echoServer) msgHandler(msg interface{}) {
	e.m.Send(e.peerAddr, msg)
}

func generateMessages() []proto.Message {
	m := make([]proto.Message, 0)

	for i := 0; i < 10; i++ {
		m1 := &example.GoGoProtobufTestMessage1{
			F0: proto.Int32(int32(rand.Int())),
			F1: proto.String(fmt.Sprintf("%10d", rand.Int())),
			F2: proto.Float32(rand.Float32()),
		}
		m = append(m, m1)

		m2 := &example.GoGoProtobufTestMessage2{
			F0: proto.Int32(int32(rand.Int())),
			F1: proto.String(fmt.Sprintf("%10d", rand.Int())),
			F2: proto.Float32(rand.Float32()),
		}
		m = append(m, m2)

		m3 := &example.GoGoProtobufTestMessage3{
			F0: proto.Int32(int32(rand.Int())),
			F1: proto.String(fmt.Sprintf("%10d", rand.Int())),
			F2: proto.String(fmt.Sprintf("%10d", rand.Int())),
		}
		m = append(m, m3)

		m4 := &example.GoGoProtobufTestMessage4{
			F0: proto.Int32(int32(rand.Int())),
			F1: proto.String(fmt.Sprintf("%10d", rand.Int())),
		}
		m = append(m, m4)
	}

	// Shuffle the messages.
	for i := range m {
		index := rand.Intn(i + 1)
		m[i], m[index] = m[index], m[i]
	}
	return m
}

// Test Send() and Recv() of the messenger.
func TestSendRecv(t *testing.T) {
	// Create the sender.
	c := codec.NewGoGoProtobufCodec()
	assert.NotNil(t, c)
	tr := transporter.NewHTTPTransporter("localhost:8008")

	// Should fail to create the messenger.
	assert.Nil(t, New(c, tr, false, false))
	m := New(c, tr, true, true)
	assert.NotNil(t, m)

	assert.NoError(t, m.RegisterMessage(&example.GoGoProtobufTestMessage1{}))
	assert.NoError(t, m.RegisterMessage(&example.GoGoProtobufTestMessage2{}))
	assert.NoError(t, m.RegisterMessage(&example.GoGoProtobufTestMessage3{}))
	assert.NoError(t, m.RegisterMessage(&example.GoGoProtobufTestMessage4{}))

	assert.NoError(t, m.RegisterHandler(&example.GoGoProtobufTestMessage1{}, handler1))
	assert.NoError(t, m.RegisterHandler(&example.GoGoProtobufTestMessage2{}, handler2))
	assert.NoError(t, m.RegisterHandler(&example.GoGoProtobufTestMessage3{}, handler3))
	assert.NoError(t, m.RegisterHandler(&example.GoGoProtobufTestMessage4{}, handler4))

	// Create the echo server.
	c = codec.NewGoGoProtobufCodec()
	assert.NotNil(t, c)
	tr = transporter.NewHTTPTransporter("localhost:8009")

	n := New(c, tr, false, true)
	assert.NotNil(t, n)

	e := &echoServer{
		m:        n,
		peerAddr: "localhost:8008",
	}

	assert.NoError(t, n.RegisterMessage(&example.GoGoProtobufTestMessage1{}))
	assert.NoError(t, n.RegisterMessage(&example.GoGoProtobufTestMessage2{}))
	assert.NoError(t, n.RegisterMessage(&example.GoGoProtobufTestMessage3{}))
	assert.NoError(t, n.RegisterMessage(&example.GoGoProtobufTestMessage4{}))

	assert.NoError(t, n.RegisterHandler(&example.GoGoProtobufTestMessage1{}, e.msgHandler))
	assert.NoError(t, n.RegisterHandler(&example.GoGoProtobufTestMessage2{}, e.msgHandler))
	assert.NoError(t, n.RegisterHandler(&example.GoGoProtobufTestMessage3{}, e.msgHandler))
	assert.NoError(t, n.RegisterHandler(&example.GoGoProtobufTestMessage4{}, e.msgHandler))

	assert.NoError(t, m.Start())
	assert.NoError(t, n.Start())

	messages := generateMessages()

	go func() {
		for i := range messages {
			m.Send("localhost:8009", messages[i])
		}
	}()

	recvMessages := make([]interface{}, 0)

	wait := make(chan struct{})
	go func() {
		for {
			select {
			case <-wait:
				return
			default:
			}
			msg, err := m.Recv()
			assert.NoError(t, err)

			recvMessages = append(recvMessages, msg)
		}
	}()
	<-time.After(time.Second * 5)
	//assert.Equal(t, messages, recvMessages)

	assert.NoError(t, m.Stop())
	assert.NoError(t, n.Stop())

	assert.NoError(t, m.Destroy())
	assert.NoError(t, n.Destroy())
}
