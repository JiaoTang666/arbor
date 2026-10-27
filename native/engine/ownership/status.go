package ownership

// BootstrapStatus describes the loaded public view, not payment authentication,
// committee liveness, evidence truth, or an independently attested node report.
// Counts and digests are immutable after bootstrap; no owner table is copied.
type BootstrapStatus struct {
	Schema          string  `json:"schema"`
	Enabled         bool    `json:"enabled"`
	Mode            string  `json:"mode"`
	RegistryVersion uint64  `json:"registry_version"`
	Owners          int     `json:"owners"`
	GenesisOwners   int     `json:"genesis_owners"`
	CertifiedOwners int     `json:"certified_owners"`
	CommitteeDigest string  `json:"committee_digest,omitempty"`
	ConfigSHA256    string  `json:"config_sha256,omitempty"`
	BundleSHA256    string  `json:"bundle_sha256,omitempty"`
	Context         Context `json:"context"`
}

func (view *View) Status() BootstrapStatus {
	status := BootstrapStatus{Schema: "coolchain/ownership-bootstrap-status-v1", Mode: "disabled"}
	if view == nil {
		return status
	}
	view.mu.RLock()
	defer view.mu.RUnlock()
	status.Enabled, status.Mode = true, "verified_public_view"
	if view.configHash != "" && view.bundleHash != "" {
		status.Mode = "pinned_offline_bundle"
	}
	if view.journalPath != "" {
		status.Mode = "verified_runtime_append_journal"
	}
	status.RegistryVersion, status.Owners = view.version, view.ownerCountLocked()
	status.GenesisOwners, status.CertifiedOwners = view.genesisOwners, len(view.owners)
	status.CommitteeDigest, status.Context = view.committeeDigest, view.context
	status.ConfigSHA256, status.BundleSHA256 = view.configHash, view.bundleHash
	return status
}
