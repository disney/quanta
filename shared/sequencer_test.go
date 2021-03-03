package shared

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSequencerBasic(t *testing.T) {

	seq := NewSequencer(1, 10)
	assert.Equal(t, seq.Maximum(), uint64(10))
	for i := 1; i < 11; i++ {
		v, ok := seq.Next()
		assert.True(t, ok)
		assert.Equal(t, v, uint64(i))
	}
	_, ok := seq.Next()
	assert.False(t, ok)
	assert.True(t, seq.IsFullySubscribed())
}
