package firstplacement

import "sort"

// TakeReadyLocalBatches is an opt-in fixed-route adapter, not a relaxation of
// TakeReady. The caller proves eligibility only for initial-ledger endpoints
// on the same shard and sends each returned group as ONE ordered wire message.
// A predecessor outside the selected same-shard group must be terminal.
func (f *Fence) TakeReadyLocalBatches(eligible map[string]int, limit int) map[int][]Flight {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := map[int][]Flight{}
	if limit <= 0 {
		return out
	}
	all := make([]*Flight, 0, len(f.flights))
	for _, x := range f.flights {
		if !x.Terminal {
			all = append(all, x)
		}
	}
	sort.Slice(all, func(i, j int) bool { return all[i].Sequence < all[j].Sequence })
	selected := map[string]int{}
	for _, x := range all {
		shard, ok := eligible[x.ID]
		if !ok || shard < 0 || !x.Held || x.Routed || len(out[shard]) >= limit || f.owners[x.Sender] != "" || f.owners[x.Recipient] != "" {
			continue
		}
		blocked := false
		for _, p := range all {
			if p.Sequence >= x.Sequence {
				break
			}
			if p.Sender == x.Sender || p.Sender == x.Recipient || p.Recipient == x.Sender || p.Recipient == x.Recipient {
				s, inBatch := selected[p.ID]
				if !inBatch || s != shard {
					blocked = true
					break
				}
			}
		}
		if blocked {
			continue
		}
		x.Held = false
		x.State = "READY"
		selected[x.ID] = shard
		out[shard] = append(out[shard], *x)
	}
	return out
}
