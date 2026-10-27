package ownership

// MemberSigner deliberately exposes only the existing local IPC operations.
// It does not introduce a generic storage engine or change the signed schema.
type MemberSigner interface {
	Vote(Batch) (Signature, error)
	AdvanceVote([]Certificate, Batch) (Signature, error)
	Commit(Certificate) (bool, error)
	Version() (uint64, error)
	Close() error
	placementRequired() bool
	storageMode() string
}

func (s *Signer) placementRequired() bool { return s.requirePlacement }
func (s *Signer) storageMode() string     { return s.registry.storageMode }
