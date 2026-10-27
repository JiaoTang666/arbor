package committee

import (
	"crypto/rand"
	"encoding/hex"
	"time"
)

const placementClockSchema = "placement_clock_v2"

// Every placement sample and decision in this process uses this one origin.
// The wall component is for display only; serialized durations use MonoNS.
var processPlacementClock = newPlacementClock()

type placementClock struct {
	origin time.Time
	id     string
}

type placementClockReading struct {
	Wall    time.Time
	ClockID string
	MonoNS  int64
}

func newPlacementClock() *placementClock {
	origin := time.Now()
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		panic(err)
	}
	return &placementClock{origin: origin, id: hex.EncodeToString(id[:])}
}

func (c *placementClock) now() placementClockReading {
	now := time.Now()
	// Both times retain Go's monotonic component. Never serialize a wall
	// timestamp and then subtract it to recover this process-local offset.
	return placementClockReading{Wall: now, ClockID: c.id, MonoNS: now.Sub(c.origin).Nanoseconds()}
}
