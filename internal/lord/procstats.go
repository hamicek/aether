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
