package shared
// Sequence generator for creating new column IDs

// Sequencer state
type Sequencer struct {
	start   uint64
	count   int
	current uint64
	Index   int // Index of item in sequencer queue, N/A client side
}

// NewSequencer constructor
func NewSequencer(start uint64, count int) *Sequencer {
	return &Sequencer{start: start, count: count, current: start}
}

// Next value.
func (s *Sequencer) Next() (nextVal uint64, ok bool) {

	if s.current < s.start+uint64(s.count) {
		ok = true
		nextVal = s.current
		s.current++
	}
	return
}

// IsFullySubscribed returns true if the sequencer has generated all values in its range.
func (s *Sequencer) IsFullySubscribed() bool {
	return !(s.current < s.start+uint64(s.count))
}

// Maximum value for this sequencer before it is fully subscribed.
func (s *Sequencer) Maximum() uint64 {
	return s.start + uint64(s.count) - 1
}
