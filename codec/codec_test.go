package codec

import (
	"testing"

	"code.google.com/p/gogoprotobuf/proto"
	example "github.com/go-distributed/messenger/codec/testexample"
	"github.com/go-distributed/testify/assert"
)

func testMarshalUnmarshal(t *testing.T, c Codec, msg interface{}) {
	b, err := c.Marshal(msg)
	assert.NoError(t, err)
	assert.NotNil(t, b)

	m, err := c.Unmarshal(b)
	assert.NoError(t, err)
	assert.Equal(t, msg, m)
}

func generateGoGoProtobufMessages() []proto.Message {
	return []proto.Message{
		&example.GoGoProtobufTestMessage1{
			F0: proto.Int32(1),
			F1: proto.String("hello"),
			F2: proto.Float32(4.2)},
		&example.GoGoProtobufTestMessage2{
			F0: proto.Int32(2),
			F1: proto.String("world"),
			F2: proto.Float32(2.4)},
		&example.GoGoProtobufTestMessage3{
			F0: proto.Int32(3),
			F1: proto.String("test"),
			F2: proto.String("4.2")},
		&example.GoGoProtobufTestMessage4{
			F0: proto.Int32(3),
			F1: proto.String("codec")},
	}
}

func TestGoGoProtobufCodec(t *testing.T) {
	c := NewGoGoProtobufCodec()
	assert.NotNil(t, c)
	assert.NoError(t, c.Initial())

	// Register messages.
	assert.NoError(t, c.RegisterMessage(&example.GoGoProtobufTestMessage1{}))
	assert.NoError(t, c.RegisterMessage(&example.GoGoProtobufTestMessage2{}))
	assert.NoError(t, c.RegisterMessage(&example.GoGoProtobufTestMessage3{}))
	assert.NoError(t, c.RegisterMessage(&example.GoGoProtobufTestMessage4{}))

	// Should fail because we have already registered once.
	assert.Error(t, c.RegisterMessage(&example.GoGoProtobufTestMessage4{}))

	// Try to marshal/unmarshal messages.
	messages := generateGoGoProtobufMessages()
	for i := range messages {
		testMarshalUnmarshal(t, c, messages[i])
	}

	// Try to marshal an unregistered message, should fail.
	_, err := c.Marshal(&example.GoGoProtobufTestMessage5{})
	assert.Error(t, err)

	assert.NoError(t, c.Destroy())
}

// Benchmark the Unmarshal() of the raw gogoprotobuf marshal,
// in order to be compared with the codec.
func BenchmarkGoGoProtobufWithoutReflectMarshal(b *testing.B) {
	var err error

	messages := generateGoGoProtobufMessages()
	data := make([][]byte, len(messages))

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for j := range messages {
			data[j], err = proto.Marshal(messages[j])
			assert.NoError(b, err)
		}
	}
}

// Benchmark the Unmarshal() of the raw gogoprotobuf,
// in order to be compared with the codec.
func BenchmarkGoGoProtobufWithoutReflectUnmarshal(b *testing.B) {
	var err error

	messages := generateGoGoProtobufMessages()
	data := make([][]byte, len(messages))

	for j := range messages {
		data[j], err = proto.Marshal(messages[j])
		assert.NoError(b, err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		m1 := new(example.GoGoProtobufTestMessage1)
		assert.NoError(b, proto.Unmarshal(data[0], m1))

		m2 := new(example.GoGoProtobufTestMessage2)
		assert.NoError(b, proto.Unmarshal(data[1], m2))

		m3 := new(example.GoGoProtobufTestMessage3)
		assert.NoError(b, proto.Unmarshal(data[2], m3))

		m4 := new(example.GoGoProtobufTestMessage4)
		assert.NoError(b, proto.Unmarshal(data[3], m4))
	}
}

// Benchmark the Marshal() of the gogoprotobuf codec.
func BenchmarkGoGoProtoBufCodecMarshal(b *testing.B) {
	var err error

	messages := generateGoGoProtobufMessages()
	data := make([][]byte, len(messages))

	c := NewGoGoProtobufCodec()
	assert.NotNil(b, c)
	assert.NoError(b, c.Initial())

	// Register messages.
	assert.NoError(b, c.RegisterMessage(&example.GoGoProtobufTestMessage1{}))
	assert.NoError(b, c.RegisterMessage(&example.GoGoProtobufTestMessage2{}))
	assert.NoError(b, c.RegisterMessage(&example.GoGoProtobufTestMessage3{}))
	assert.NoError(b, c.RegisterMessage(&example.GoGoProtobufTestMessage4{}))

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for j := range messages {
			data[j], err = c.Marshal(messages[j])
			assert.NoError(b, err)
		}
	}
}

// Benchmark the Unmarshal() of the gogoprotobuf codec.
func BenchmarkGoGoProtoBufCodecUnmarshal(b *testing.B) {
	var err error

	messages := generateGoGoProtobufMessages()
	data := make([][]byte, len(messages))

	c := NewGoGoProtobufCodec()
	assert.NotNil(b, c)
	assert.NoError(b, c.Initial())

	// Register messages.
	assert.NoError(b, c.RegisterMessage(&example.GoGoProtobufTestMessage1{}))
	assert.NoError(b, c.RegisterMessage(&example.GoGoProtobufTestMessage2{}))
	assert.NoError(b, c.RegisterMessage(&example.GoGoProtobufTestMessage3{}))
	assert.NoError(b, c.RegisterMessage(&example.GoGoProtobufTestMessage4{}))

	for j := range messages {
		data[j], err = c.Marshal(messages[j])
		assert.NoError(b, err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for j := range data {
			_, err := c.Unmarshal(data[j])
			assert.NoError(b, err)
		}
	}
}
