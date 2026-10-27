package firstplacement

import (
	"fmt"
	"sort"
)

// Atomic conservation check; neither a timeout nor a successful send is terminal.
func (f *Fence) CompletedRouteShards(expected int) ([]int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if expected <= 0 || len(f.flights) != expected || len(f.owners) != 0 {
		return nil, fmt.Errorf("input_or_creation_not_drained")
	}
	seen := map[int]bool{}
	for _, x := range f.flights {
		if !x.Terminal || !x.Routed || x.Held || x.SourceShard < 0 || x.TargetShard < 0 {
			return nil, fmt.Errorf("occurrence_not_terminal")
		}
		seen[x.SourceShard] = true
		seen[x.TargetShard] = true
	}
	out := []int{}
	for s := range seen {
		out = append(out, s)
	}
	sort.Ints(out)
	return out, nil
}
