package server

import (
	"github.com/stretchr/testify/assert"
	"github.com/disney/quanta/shared"
	"testing"
)

func TestSequencerQueue(t *testing.T) {

	queue := NewSequencerQueue()
	for i := 0; i < 10; i++ {
		seq := shared.NewSequencer(uint64(i*10+1), 10)
		assert.Equal(t, seq.Maximum(), uint64((i+1)*10))
		queue.Push(seq)
	}
	assert.Equal(t, 10, queue.Len())
	assert.Equal(t, queue.Peek().Maximum(), uint64(100))
	for i := 0; i < 10; i++ {
		peek := queue.Peek()
		pop := queue.Pop()
		assert.Equal(t, peek.Maximum(), pop.Maximum())
	}
	assert.Equal(t, queue.Len(), 0)
	// Create reverse order
	for i := 9; i >= 0; i-- {
		seq := shared.NewSequencer(uint64(i*10+1), 10)
		assert.Equal(t, seq.Maximum(), uint64((i+1)*10))
		queue.Push(seq)
	}
	assert.Equal(t, queue.Len(), 10)
	assert.Equal(t, queue.Peek().Maximum(), uint64(100))
	for i := 0; i < 10; i++ {
		peek := queue.Peek()
		pop := queue.Pop()
		assert.Equal(t, peek.Maximum(), pop.Maximum())
	}
	assert.Equal(t, queue.Len(), 0)
	for i := 0; i < 10; i++ {
		seq := shared.NewSequencer(uint64(i*10+1), 10)
		queue.Push(seq)
	}
	x := queue.Remove(5)
	assert.Equal(t, 9, queue.Len())
	assert.Equal(t, uint64(60), x.Maximum())

	// Push it back
	queue.Push(x)
	assert.Equal(t, 10, queue.Len())
	assert.Equal(t, uint64(100), queue.Maximum())

	// Purge tests
	queue.Purge(59)
	assert.Equal(t, 5, queue.Len())
	assert.Equal(t, uint64(100), queue.Maximum())

	queue.Purge(70)
	assert.Equal(t, 3, queue.Len())
	assert.Equal(t, uint64(100), queue.Maximum())

	queue.Purge(0)
	assert.Equal(t, 3, queue.Len())
	assert.Equal(t, uint64(100), queue.Maximum())

	queue.Purge(1000)
	assert.Equal(t, 0, queue.Len())
	assert.Equal(t, uint64(0), queue.Maximum())

}
