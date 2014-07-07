package codec

// Codec defines the interface that a codec should implement.
// A codec should be able to marshal/unmarshal messages from the
// given bytes.
type Codec interface {
	// Initiate a codec.
	Initial() error

	// Register a message type.
	RegisterMessage(msg interface{}) error

	// Marshal a message into bytes.
	Marshal(msg interface{}) ([]byte, error)

	// Unmarshal a message from bytes.
	Unmarshal(data []byte) (interface{}, error)

	// Destroy a codec, release the resource.
	Destroy() error
}
