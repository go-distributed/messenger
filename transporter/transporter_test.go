package transporter

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/go-distributed/messenger/3rdparty/github.com/testify/assert"
)

// Help to generage random bytes.
func generateRandomBytes(m, n int) [][]byte {
	data := make([][]byte, m)
	for i := range data {
		data[i] = make([]byte, rand.Intn(n)+100)
		for j := range data[i] {
			data[i][j] = byte(rand.Int())
		}
	}
	return data
}

// Test the transporter's interface.
func testTransporter(t *testing.T, s, r Transporter, target string) {
	expectedData := generateRandomBytes(2048, 1024)
	actualData := make([][]byte, len(expectedData))

	done := make(chan struct{})
	// Send.
	go func() {
		for i := range expectedData {
			assert.NoError(t, s.Send(target, expectedData[i]))
		}
	}()

	// Receive.
	go func() {
		for i := 0; i < len(expectedData); i++ {
			b, err := r.Recv()
			assert.NoError(t, err)
			actualData[i] = b
		}
		close(done)
	}()

	select {
	case <-time.After(time.Second * 10):
		t.Fatal("Not enough messages received, waited 10s")
	case <-done:
		assert.Equal(t, expectedData, actualData)
	}

	// Make sure there is no pending messages.
	finish := make(chan struct{})
	go func() {
		r.Recv()
		close(finish)
	}()

	select {
	case <-finish:
		t.Fatal("Receiving unexpected message")
	case <-time.After(time.Second * 5):
	}

	assert.NoError(t, s.Stop())
	assert.NoError(t, s.Destroy())
	assert.NoError(t, r.Stop())
	assert.NoError(t, r.Destroy())
}

// Benchmark the transporter's Send() and Recv().
func benchmarkTransporter(b *testing.B, s, r Transporter, target string) {
	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte(rand.Int())
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Send.
		assert.NoError(b, s.Send(target, data))
		_, err := r.Recv()
		assert.NoError(b, err)
	}
}

// Test the HTTPTransporter.
func TestHTTPTransporter(t *testing.T) {
	sender := NewHTTPTransporter("localhost:8080")
	assert.NotNil(t, sender)

	receiver := NewHTTPTransporter("localhost:8081")
	assert.NotNil(t, receiver)

	go func() {
		assert.NoError(t, sender.Start())
	}()
	go func() {
		assert.NoError(t, receiver.Start())
	}()

	time.Sleep(time.Second)

	testTransporter(t, sender, receiver, "localhost:8081")
}

// Benchmark the HTTPTransporter.
func BenchmarkHTTPTransporter(b *testing.B) {
	// Use random port to avoid port collision (hopefully).
	port := rand.Intn(100) + 8000
	s := fmt.Sprintf("localhost:%d", port)
	r := fmt.Sprintf("localhost:%d", port+1)
	sender := NewHTTPTransporter(s)
	assert.NotNil(b, sender)

	receiver := NewHTTPTransporter(r)
	assert.NotNil(b, receiver)

	go func() {
		assert.NoError(b, sender.Start())
	}()
	go func() {
		assert.NoError(b, receiver.Start())
	}()

	time.Sleep(time.Second)

	benchmarkTransporter(b, sender, receiver, r)
}
