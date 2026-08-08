package lord

// RestartDecision determines what to do after a thrall exits.
type RestartDecision int

const (
	DontRestart RestartDecision = iota
	RestartOne
	RestartAll  // one_for_all
	RestartRest // rest_for_one
)

// decide evaluates the child's restart policy + the tree strategy.
// abnormal = exit code != 0 or a crash outside a controlled shutdown.
func decide(restart, strategy string, abnormal bool) RestartDecision {
	switch restart {
	case "temporary":
		return DontRestart
	case "transient":
		if !abnormal {
			return DontRestart
		}
	case "permanent":
		// always restart
	}
	switch strategy {
	case "one_for_all":
		return RestartAll
	case "rest_for_one":
		return RestartRest
	default:
		return RestartOne
	}
}
