package codec

// A codec interface that defines what a codec should implement.
// A codec should be able to marshal/unmarshal through the given
// connection.
type Codec interface {
	// Init a codec.
	Initial() error

	// Register a message type.
	Register(msg interface{})

	// Marshal a message into bytes.
	Marshal(msg interface{}) ([]byte, error)

	// Unmarshal a message from bytes.
	Unmarshal(data []byte) (interface{}, error)

	// Destroy a codec, release the resource.
	Destroy() error
}
