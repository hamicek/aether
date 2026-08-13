package lord

// Process resource sampling for the observer dashboard: each thrall's resident memory (RSS) and
// CPU %. A thrall runs as `sh -c '<cmd>'` in its own process group (Setpgid), so the PID the lord
// holds (child.pid()) is the sh leader - its RSS/CPU is tiny. The real load is the interpreter
// grandchild, so both are summed over the WHOLE process group, keyed by pgid. One `ps` per poll
// samples every process; the poll then looks up each thrall's pgid.

import (
	"bufio"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// procAgg is the aggregated resource use of one process group: summed resident memory (bytes) and
// summed cumulative CPU time (seconds) across every process sharing the pgid.
type procAgg struct {
	RSSBytes   int64
	CPUSeconds float64
}

// parseCPUTime parses a `ps -o time` value into seconds. The value is colon-separated with the
// seconds (possibly fractional) last, then minutes and hours, and an optional leading `DD-` day
// count - so it handles macOS `MMM:SS.ss` and Linux `[[DD-]HH:]MM:SS` alike.
func parseCPUTime(s string) float64 {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}
	var days float64
	if i := strings.IndexByte(s, '-'); i >= 0 {
		days, _ = strconv.ParseFloat(s[:i], 64)
		s = s[i+1:]
	}
	parts := strings.Split(s, ":")
	mult := []float64{1, 60, 3600} // seconds, minutes, hours - applied from the right
	var total float64
	for i := 0; i < len(parts) && i < len(mult); i++ {
		v, _ := strconv.ParseFloat(parts[len(parts)-1-i], 64)
		total += v * mult[i]
	}
	return total + days*86400
}

// parseProcStats parses `ps -A -o pgid=,rss=,time=` output into per-process-group aggregates: RSS
// converted from KB to bytes, CPU time summed, both grouped by pgid.
func parseProcStats(psOut string) map[int]procAgg {
	out := map[int]procAgg{}
	sc := bufio.NewScanner(strings.NewReader(psOut))
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) < 3 {
			continue
		}
		pgid, err := strconv.Atoi(fields[0])
		if err != nil {
			continue
		}
		rssKB, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			continue
		}
		agg := out[pgid]
		agg.RSSBytes += rssKB * 1024
		agg.CPUSeconds += parseCPUTime(fields[2])
		out[pgid] = agg
	}
	return out
}

// sampleProcStats runs ps once and returns per-process-group aggregates. A single subprocess per
// poll, independent of the thrall count.
func sampleProcStats() (map[int]procAgg, error) {
	out, err := exec.Command("ps", "-A", "-o", "pgid=,rss=,time=").Output()
	if err != nil {
		return nil, err
	}
	return parseProcStats(string(out)), nil
}

// cpuPercent computes CPU usage as the fraction of wall time spent on CPU between two samples, in
// percent. It returns 0 for a non-positive interval or a CPU-time decrease (a restarted process
// whose counter reset), so a fresh or restarted thrall never reports a bogus value.
func cpuPercent(prevCPU, curCPU float64, elapsed time.Duration) float64 {
	sec := elapsed.Seconds()
	if sec <= 0 || curCPU < prevCPU {
		return 0
	}
	return (curCPU - prevCPU) / sec * 100
}

// cpuSample is a thrall's previous CPU time and when it was taken, for the percentage delta.
type cpuSample struct {
	cpuSeconds float64
	at         time.Time
}

// pollProcStats periodically samples each live thrall's process-group RSS and CPU and records
// them. It runs from Start, independent of the NATS mode. A failed ps sample is skipped, never
// fatal, so it cannot stall supervision. The previous CPU time per thrall lives here (only this
// goroutine touches it), so the CPU percentage is a delta over the poll interval.
func (l *Lord) pollProcStats() {
	if l.procStatsPollEvery <= 0 {
		return
	}
	prev := map[string]cpuSample{}
	t := time.NewTicker(l.procStatsPollEvery)
	defer t.Stop()
	for {
		select {
		case <-l.appCtx.Done():
			return
		case now := <-t.C:
			l.pollProcStatsOnce(prev, now)
		}
	}
}

// pollProcStatsOnce samples ps once and records RSS + CPU% for every live thrall, keyed by its
// process group (pgid == the leader pid, thanks to Setpgid). prev carries the last CPU time per
// thrall for the delta; entries for thralls no longer live are dropped, so a restarted thrall
// (fresh process, reset counter) starts clean instead of producing a bogus delta.
func (l *Lord) pollProcStatsOnce(prev map[string]cpuSample, now time.Time) {
	agg, err := sampleProcStats()
	if err != nil {
		return // ps unavailable this tick - skip, never disturb supervision
	}

	type liveThrall struct {
		name string
		pgid int
	}
	l.childrenMu.RLock()
	lives := make([]liveThrall, 0, len(l.children))
	for _, ch := range l.children {
		if ch.live.Load() {
			lives = append(lives, liveThrall{ch.spec.Name, ch.pid()})
		}
	}
	l.childrenMu.RUnlock()

	seen := make(map[string]struct{}, len(lives))
	for _, lv := range lives {
		seen[lv.name] = struct{}{}
		a, ok := agg[lv.pgid]
		if !ok {
			continue // process group not visible to ps this tick
		}
		cpu := 0.0
		if p, ok := prev[lv.name]; ok {
			cpu = cpuPercent(p.cpuSeconds, a.CPUSeconds, now.Sub(p.at))
		}
		prev[lv.name] = cpuSample{cpuSeconds: a.CPUSeconds, at: now}
		l.metrics.recordProcStats(lv.name, a.RSSBytes, cpu)
	}
	for name := range prev {
		if _, ok := seen[name]; !ok {
			delete(prev, name)
		}
	}
}
