// SCADA spike - site thrall (AE-014).
//
// Coarse-grained mapping: ONE thrall holds a whole site's process image as a Go map
// in memory (not one thrall per tag). It ingests telemetry casts, tracks sequence
// gaps (slow-consumer drops) and processing latency, raises a threshold alarm, and
// answers a snapshot/stats call. This is throwaway spike code, not production runtime.
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/hamicek/aether/sdk/go/thrall"
)

// nowNs is the wall clock in nanoseconds. Handlers pass it into the pure logic so the
// ingest path can be tested with controlled timestamps.
func nowNs() int64 { return time.Now().UnixNano() }

// alarmSubject is where the site thrall publishes threshold-breach events. The bench
// harness (and `aether events`-style tooling) can subscribe to measure alarm latency.
const alarmSubject = "scada.alarm"

// tele is the telemetry payload a driver casts for each measured value. Seq is a
// per-tag monotonic counter (gap detection); TsNs is the driver's send time (latency).
type tele struct {
	Tag   string  `json:"tag"`
	Value float64 `json:"value"`
	Seq   uint64  `json:"seq"`
	TsNs  int64   `json:"ts_ns"`
}

// alarm is the event emitted when a tag crosses the threshold upward.
type alarm struct {
	Tag      string  `json:"tag"`
	Value    float64 `json:"value"`
	BreachNs int64   `json:"breach_ns"`
}

// stats is the reply to the "stats" call - what the harness reads to find the ceiling.
type stats struct {
	Received     uint64 `json:"received"`
	Gaps         uint64 `json:"gaps"`
	Tags         int    `json:"tags"`
	MaxProcNs    int64  `json:"max_proc_ns"`
	SumProcNs    int64  `json:"sum_proc_ns"`
	Alarms       uint64 `json:"alarms"`
	SlowConsumer bool   `json:"slow_consumer"`
}

// site is the in-memory process image plus ingest bookkeeping. The thrall mailbox is
// serialized by the SDK, so handlers never run concurrently and need no locks here.
type site struct {
	image     map[string]float64 // the process image: last value per tag
	lastSeq   map[string]uint64  // per-tag last seq, for gap detection
	over      map[string]bool    // per-tag alarm latch (hysteresis: fire only on cross)
	threshold float64
	received  uint64
	gaps      uint64
	maxProcNs int64
	sumProcNs int64
	alarms    uint64
}

func newSite(threshold float64) *site {
	return &site{
		image:     make(map[string]float64),
		lastSeq:   make(map[string]uint64),
		over:      make(map[string]bool),
		threshold: threshold,
	}
}

// ingest applies one telemetry sample to the image and returns an alarm to publish
// (or nil). nowNs is passed in (not read from the clock) so the logic is testable and
// time-driven, per the SCADA design's "time of data, not server clock" rule.
func (s *site) ingest(t tele, nowNs int64) *alarm {
	s.received++

	// Gap detection: a jump in the per-tag seq means casts were dropped upstream.
	if prev, ok := s.lastSeq[t.Tag]; ok && t.Seq > prev+1 {
		s.gaps += t.Seq - prev - 1
	}
	s.lastSeq[t.Tag] = t.Seq

	// Processing latency: how long from the driver's send to handling here.
	if t.TsNs > 0 {
		if lat := nowNs - t.TsNs; lat > 0 {
			if lat > s.maxProcNs {
				s.maxProcNs = lat
			}
			s.sumProcNs += lat
		}
	}

	s.image[t.Tag] = t.Value

	// Threshold guard with hysteresis: fire only on the transition below -> above.
	wasOver := s.over[t.Tag]
	isOver := t.Value > s.threshold
	s.over[t.Tag] = isOver
	if isOver && !wasOver {
		s.alarms++
		return &alarm{Tag: t.Tag, Value: t.Value, BreachNs: nowNs}
	}
	return nil
}

func (s *site) stats(slowConsumer bool) stats {
	return stats{
		Received:     s.received,
		Gaps:         s.gaps,
		Tags:         len(s.image),
		MaxProcNs:    s.maxProcNs,
		SumProcNs:    s.sumProcNs,
		Alarms:       s.alarms,
		SlowConsumer: slowConsumer,
	}
}

// snapshot returns a copy of the process image - a realistic read for a BFF/UI, so
// its call latency reflects real serialization cost.
func (s *site) snapshot() map[string]float64 {
	out := make(map[string]float64, len(s.image))
	for k, v := range s.image {
		out[k] = v
	}
	return out
}

func thresholdFromEnv() float64 {
	if v := os.Getenv("SCADA_THRESHOLD"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return 100.0
}

func main() {
	s := newSite(thresholdFromEnv())

	def := thrall.Def[*site]{
		Name: os.Getenv("AETHER_NAME"),
		Init: func(_ *thrall.Ctx) (*site, error) { return s, nil },

		HandleCast: map[string]thrall.CastFn[*site]{
			"tele": func(payload json.RawMessage, st *site, ctx *thrall.Ctx) (*site, error) {
				var t tele
				if err := json.Unmarshal(payload, &t); err != nil {
					return st, fmt.Errorf("tele payload: %w", err)
				}
				if a := st.ingest(t, nowNs()); a != nil {
					data, err := json.Marshal(a)
					if err != nil {
						return st, fmt.Errorf("alarm marshal: %w", err)
					}
					if err := ctx.NATS.Publish(alarmSubject, data); err != nil {
						return st, fmt.Errorf("alarm publish: %w", err)
					}
				}
				return st, nil
			},
		},

		HandleCall: map[string]thrall.CallFn[*site]{
			"snapshot": func(_ json.RawMessage, st *site, _ *thrall.Ctx) (any, *site, error) {
				return st.snapshot(), st, nil
			},
			"stats": func(_ json.RawMessage, st *site, _ *thrall.Ctx) (any, *site, error) {
				return st.stats(false), st, nil
			},
		},

		Terminate: func(reason string, st *site) {
			fmt.Printf("site exiting (%s): received=%d gaps=%d alarms=%d tags=%d\n",
				reason, st.received, st.gaps, st.alarms, len(st.image))
		},
	}

	if err := thrall.Start(def); err != nil {
		log.Fatal(err)
	}
}
