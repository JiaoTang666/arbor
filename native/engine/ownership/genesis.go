package ownership

import "fmt"

// Genesis is a private, immutable historical ownership index. Constructors
// copy the caller's map; no method exports the backing map. It can therefore
// be shared by the ledger and authorization view without shared mutable state.
type Genesis struct {
	routes map[string]uint64
	digest string
	shards uint64
}

func NewGenesis(routes map[string]uint64, shards uint64) (*Genesis, error) {
	if shards == 0 {
		return nil, fmt.Errorf("invalid genesis topology")
	}
	owned := make(map[string]uint64, len(routes))
	for account, shard := range routes {
		owned[account] = shard
	}
	digest, err := RoutesDigest(owned, shards)
	if err != nil {
		return nil, err
	}
	return &Genesis{routes: owned, digest: digest, shards: shards}, nil
}

// Owner takes a canonical account, as supplied by the ledger or verifier.
func (g *Genesis) Owner(account string) (uint64, bool) {
	if g == nil {
		return 0, false
	}
	owner, exists := g.routes[account]
	return owner, exists
}

func (g *Genesis) Len() int {
	if g == nil {
		return 0
	}
	return len(g.routes)
}

// ownerLocked and ownerCountLocked require view.mu, or an unpublished view.
func (view *View) ownerLocked(account string) (uint64, bool) {
	if owner, exists := view.owners[account]; exists {
		return owner, true
	}
	return view.genesis.Owner(account)
}

func (view *View) ownerCountLocked() int { return view.genesis.Len() + len(view.owners) }

func (view *View) rangeOwnersLocked(visit func(string, uint64) error) error {
	for account, shard := range view.genesis.routes {
		if err := visit(account, shard); err != nil {
			return err
		}
	}
	for account, shard := range view.owners {
		if err := visit(account, shard); err != nil {
			return err
		}
	}
	return nil
}

// ownerSnapshot is for legacy durable-store caches only, never transaction
// admission. The returned full map is independent of the view and its base.
func (view *View) ownerSnapshot() map[string]uint64 {
	owners := make(map[string]uint64, view.ownerCountLocked())
	view.rangeOwnersLocked(func(account string, shard uint64) error {
		owners[account] = shard
		return nil
	})
	return owners
}

// PartitionCounts reports the logical table, without materializing a full map.
func (view *View) PartitionCounts(shard uint64) (int, int) {
	view.mu.RLock()
	defer view.mu.RUnlock()
	local := 0
	view.rangeOwnersLocked(func(_ string, owner uint64) error {
		if owner == shard {
			local++
		}
		return nil
	})
	return local, view.ownerCountLocked()
}
