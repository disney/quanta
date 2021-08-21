package server

// Sequence generator creating column IDs

import (
	"github.com/disney/quanta/shared"
	"sort"
)

// SequencerQueue data structure.
type SequencerQueue struct {
	queue []*shared.Sequencer
}

// NewSequencerQueue - Construct a new SequencerQueue.
func NewSequencerQueue() *SequencerQueue {
	return &SequencerQueue{}
}

// List sequence generators.
func (s *SequencerQueue) List() []*shared.Sequencer {
	return s.queue
}

func (s *SequencerQueue) Len() int {
	return len(s.queue)
}

func (s *SequencerQueue) Less(i, j int) bool {
	// We want Pop to give us the highest, not lowest, priority so we use greater than here.
	return s.queue[i].Maximum() < s.queue[j].Maximum()
}

func (s *SequencerQueue) Swap(i, j int) {
	s.queue[i], s.queue[j] = s.queue[j], s.queue[i]
	s.queue[i].Index = i
	s.queue[j].Index = j
}

// Push a new sequencer on the queue.
func (s *SequencerQueue) Push(item *shared.Sequencer) {
	if s.queue == nil {
		s.queue = make([]*shared.Sequencer, 0)
	}
	s.queue = append(s.queue, item)
	sort.Sort(s)
}

// Remove a sequencer at index position.
func (s *SequencerQueue) Remove(index int) *shared.Sequencer {
	item := s.queue[index]
	s.queue = append(s.queue[:index], s.queue[index+1:]...)
	return item
}

// Pop - remove a sequencer from the tail of the queue and return it.
func (s *SequencerQueue) Pop() *shared.Sequencer {
	return s.Remove(s.Len() - 1)
}

// Peek at the tail of the queue.
func (s *SequencerQueue) Peek() *shared.Sequencer {
	return s.queue[(s.Len() - 1)]
}

// Purge the queue
func (s *SequencerQueue) Purge(max uint64) {

	for s.Len() > 0 {
		if s.queue[0].Maximum() <= max {
			// remove item
			s.queue = append(s.queue[:0], s.queue[1:]...)
		} else {
			break
		}
	}
}

// Maximum - Return highest value.
func (s *SequencerQueue) Maximum() uint64 {
	if s.Len() == 0 {
		return 0
	}
	return s.queue[(s.Len() - 1)].Maximum()
}
