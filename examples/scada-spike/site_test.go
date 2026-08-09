package main

import "testing"

// TestIngestUpdatesImage: a sample lands in the image under its tag.
func TestIngestUpdatesImage(t *testing.T) {
	s := newSite(100)
	s.ingest(tele{Tag: "p1", Value: 2.5, Seq: 1}, 0)
	if got := s.image["p1"]; got != 2.5 {
		t.Fatalf("image[p1] = %v, want 2.5", got)
	}
	if s.received != 1 {
		t.Fatalf("received = %d, want 1", s.received)
	}
}

// TestIngestDetectsGaps: a jump in the per-tag seq counts the dropped samples.
func TestIngestDetectsGaps(t *testing.T) {
	s := newSite(100)
	s.ingest(tele{Tag: "p1", Value: 1, Seq: 1}, 0)
	s.ingest(tele{Tag: "p1", Value: 1, Seq: 2}, 0) // contiguous, no gap
	s.ingest(tele{Tag: "p1", Value: 1, Seq: 5}, 0) // jump 2 -> 5, 2 missing
	if s.gaps != 2 {
		t.Fatalf("gaps = %d, want 2", s.gaps)
	}
}

// TestGapsArePerTag: interleaved tags each track their own sequence.
func TestGapsArePerTag(t *testing.T) {
	s := newSite(100)
	s.ingest(tele{Tag: "a", Value: 1, Seq: 1}, 0)
	s.ingest(tele{Tag: "b", Value: 1, Seq: 1}, 0)
	s.ingest(tele{Tag: "a", Value: 1, Seq: 2}, 0)
	s.ingest(tele{Tag: "b", Value: 1, Seq: 2}, 0)
	if s.gaps != 0 {
		t.Fatalf("gaps = %d, want 0 (interleaved tags are not gaps)", s.gaps)
	}
}

// TestAlarmFiresOnceOnCross: hysteresis - the alarm fires on the below->above
// transition and stays silent while the value remains above the threshold.
func TestAlarmFiresOnceOnCross(t *testing.T) {
	s := newSite(100)
	if a := s.ingest(tele{Tag: "p1", Value: 50, Seq: 1}, 10); a != nil {
		t.Fatalf("below threshold should not alarm, got %+v", a)
	}
	a := s.ingest(tele{Tag: "p1", Value: 150, Seq: 2}, 20)
	if a == nil {
		t.Fatal("crossing above threshold should alarm")
	}
	if a.BreachNs != 20 || a.Value != 150 {
		t.Fatalf("alarm = %+v, want BreachNs=20 Value=150", a)
	}
	if a2 := s.ingest(tele{Tag: "p1", Value: 160, Seq: 3}, 30); a2 != nil {
		t.Fatalf("still above threshold should not re-alarm, got %+v", a2)
	}
	if s.alarms != 1 {
		t.Fatalf("alarms = %d, want 1", s.alarms)
	}
}

// TestAlarmReArmsAfterDrop: once the value drops below, a later cross alarms again.
func TestAlarmReArmsAfterDrop(t *testing.T) {
	s := newSite(100)
	s.ingest(tele{Tag: "p1", Value: 150, Seq: 1}, 10) // alarm 1
	s.ingest(tele{Tag: "p1", Value: 50, Seq: 2}, 20)  // clears
	if a := s.ingest(tele{Tag: "p1", Value: 150, Seq: 3}, 30); a == nil {
		t.Fatal("re-crossing after a drop should alarm again")
	}
	if s.alarms != 2 {
		t.Fatalf("alarms = %d, want 2", s.alarms)
	}
}

// TestProcLatencyTracked: processing latency (now - send) feeds max and sum.
func TestProcLatencyTracked(t *testing.T) {
	s := newSite(100)
	s.ingest(tele{Tag: "p1", Value: 1, Seq: 1, TsNs: 100}, 400) // lat 300
	s.ingest(tele{Tag: "p1", Value: 1, Seq: 2, TsNs: 500}, 600) // lat 100
	if s.maxProcNs != 300 {
		t.Fatalf("maxProcNs = %d, want 300", s.maxProcNs)
	}
	if s.sumProcNs != 400 {
		t.Fatalf("sumProcNs = %d, want 400", s.sumProcNs)
	}
}

// TestSnapshotIsCopy: the snapshot must not alias the live image.
func TestSnapshotIsCopy(t *testing.T) {
	s := newSite(100)
	s.ingest(tele{Tag: "p1", Value: 1, Seq: 1}, 0)
	snap := s.snapshot()
	snap["p1"] = 999
	if s.image["p1"] != 1 {
		t.Fatal("snapshot aliases the live image")
	}
}
