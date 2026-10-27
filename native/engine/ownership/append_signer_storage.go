package ownership

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/gofrs/flock"
)

const appendWALSchema = "coolchain/ownership-signer-wal-v1"
const appendFrameSize = 112
const maxAppendPayload = 64 << 20

var appendMagic = [8]byte{'C', 'C', 'S', 'W', 'A', 'L', '1', 0}
var errAppendShortTail = errors.New("incomplete final WAL frame")

type appendHeader struct {
	Schema          string `json:"schema"`
	StorageMode     string `json:"storage_mode"`
	Configuration   Config `json:"configuration"`
	Member          Member `json:"member"`
	PlacementPolicy string `json:"placement_policy,omitempty"`
}

type appendFrame struct {
	payload []byte
	hash    [32]byte
	next    int64
}

// Lock the same regular inode as the store, not a replaceable sibling lockfile.
// Path/key cloning or replacement by an operator remain outside the model.
func openAppendFile(path string) (*os.File, *flock.Flock, bool, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	created := err == nil
	if created {
		if err = file.Close(); err != nil {
			return nil, nil, false, err
		}
	} else if !os.IsExist(err) {
		return nil, nil, false, err
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		return nil, nil, false, fmt.Errorf("WAL requires a private regular file")
	}
	lock := flock.New(path)
	locked, err := lock.TryLock()
	if err != nil || !locked {
		lock.Close()
		return nil, nil, false, fmt.Errorf("signer WAL is locked or unavailable")
	}
	file, err = os.OpenFile(path, os.O_RDWR, 0600)
	if err == nil {
		opened, statErr := file.Stat()
		if statErr != nil || !os.SameFile(info, opened) {
			err = fmt.Errorf("signer WAL pathname changed while locking")
		}
	}
	if err != nil {
		if file != nil {
			file.Close()
		}
		lock.Close()
		return nil, nil, false, err
	}
	return file, lock, created, nil
}

func (s *AppendSigner) header() appendHeader {
	header := appendHeader{Schema: appendWALSchema, StorageMode: AppendSignerMode,
		Configuration: s.verifier.config, Member: Member{ID: s.memberID, PublicKey: s.verifier.keys[s.memberID]}}
	if s.requirePlacement {
		header.PlacementPolicy = PlacementWitnessSchema
	}
	return header
}

func syncAppendDirectory(path string) error {
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

func (s *AppendSigner) initialize() error {
	raw, err := json.Marshal(s.header())
	if err != nil {
		return err
	}
	frame, err := s.appendFrame(raw)
	if err != nil {
		return err
	}
	// One additional namespace barrier ONLY when a new file is created.
	if err := syncAppendDirectory(s.path); err != nil {
		return err
	}
	s.head, s.end = frame.hash, frame.next
	return nil
}

func decodeAppend(raw []byte, value interface{}) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	if decoder.Decode(new(interface{})) != io.EOF {
		return fmt.Errorf("trailing WAL record data")
	}
	return nil
}

func readAppendFrame(file *os.File, offset, size int64, previous [32]byte) (appendFrame, error) {
	var frame appendFrame
	if size-offset < appendFrameSize {
		return frame, errAppendShortTail
	}
	prefix := make([]byte, appendFrameSize)
	if _, err := file.ReadAt(prefix, offset); err != nil {
		return frame, err
	}
	// Verify the fixed header BEFORE trusting length or treating an EOF as a
	// crash tail. A corrupt length must never erase a complete durable vote.
	headerHash := sha256.Sum256(prefix[:80])
	if !bytes.Equal(headerHash[:], prefix[80:112]) {
		return frame, fmt.Errorf("complete WAL frame header checksum corrupt")
	}
	length := binary.BigEndian.Uint64(prefix[8:16])
	if !bytes.Equal(prefix[:8], appendMagic[:]) || !bytes.Equal(prefix[16:48], previous[:]) || length == 0 || length > maxAppendPayload {
		return frame, fmt.Errorf("WAL frame magic/chain/length corrupt")
	}
	if uint64(size-offset-appendFrameSize) < length {
		return frame, errAppendShortTail
	}
	frame.payload = make([]byte, int(length))
	if _, err := file.ReadAt(frame.payload, offset+appendFrameSize); err != nil {
		return frame, err
	}
	hash := sha256.New()
	hash.Write(prefix[:48])
	hash.Write(frame.payload)
	copy(frame.hash[:], hash.Sum(nil))
	if !bytes.Equal(frame.hash[:], prefix[48:80]) {
		return frame, fmt.Errorf("complete WAL frame checksum corrupt")
	}
	frame.next = offset + appendFrameSize + int64(length)
	return frame, nil
}

// appendFrame does not publish any indexes or signature. Even an error AFTER
// Sync fences this instance; reopening determines whether the frame survived.
func (s *AppendSigner) appendFrame(raw []byte) (appendFrame, error) {
	var frame appendFrame
	if len(raw) == 0 || len(raw) > maxAppendPayload {
		return frame, fmt.Errorf("WAL payload bound exceeded")
	}
	info, err := s.file.Stat()
	if err != nil || info.Size() != s.end {
		s.fenced = true
		return frame, fmt.Errorf("WAL length changed after opening")
	}
	prefix := make([]byte, appendFrameSize)
	copy(prefix[:8], appendMagic[:])
	binary.BigEndian.PutUint64(prefix[8:16], uint64(len(raw)))
	copy(prefix[16:48], s.head[:])
	hash := sha256.New()
	hash.Write(prefix[:48])
	hash.Write(raw)
	copy(frame.hash[:], hash.Sum(nil))
	copy(prefix[48:], frame.hash[:])
	headerHash := sha256.Sum256(prefix[:80])
	copy(prefix[80:], headerHash[:])
	packet := append(prefix, raw...)
	if s.beforeAppend != nil {
		err = s.beforeAppend()
	}
	if err == nil {
		var count int
		count, err = s.file.WriteAt(packet, s.end)
		if err == nil && count != len(packet) {
			err = io.ErrShortWrite
		}
	}
	if err == nil && s.afterWrite != nil {
		err = s.afterWrite()
	}
	if err == nil {
		err = s.file.Sync() // Exactly one durability barrier per normal transition.
	}
	if err == nil && s.afterAppend != nil {
		err = s.afterAppend()
	}
	if err != nil {
		s.fenced = true
		return frame, err
	}
	frame.next = s.end + int64(len(packet))
	return frame, nil
}

func (s *AppendSigner) commitPlan(plan appendPlan) error {
	if len(plan.parents) == 0 && plan.vote == nil {
		return nil // Exact retries perform NO write and NO Sync.
	}
	if s.index == ^uint64(0) {
		return fmt.Errorf("WAL record index exhausted")
	}
	event := appendTransition{Schema: appendWALSchema, Index: s.index + 1, BaseVersion: s.version, Vote: plan.vote}
	for _, parent := range plan.parents {
		event.Parents = append(event.Parents, parent.certificate)
	}
	raw, err := json.Marshal(event)
	if err != nil {
		return err
	}
	frame, err := s.appendFrame(raw)
	if err != nil {
		return err
	}
	return s.publish(plan, s.end, frame, nil)
}

func (s *AppendSigner) publish(plan appendPlan, offset int64, frame appendFrame, view *View) error {
	if view != nil {
		for _, parent := range plan.parents {
			// prepare already checked exactly this prefix; Snapshot owns the view.
			changed, err := view.apply(parent.certificate, parent.digest)
			if err != nil || !changed {
				s.fenced = true
				return fmt.Errorf("verified Snapshot prefix failed to apply: %v", err)
			}
		}
	} else {
		for account, shard := range plan.owners {
			s.ownerCache[account] = shard
		}
		for proposal, digest := range plan.proposals {
			s.proposals[proposal] = digest
		}
	}
	if plan.vote != nil {
		s.votes[plan.vote.Batch.BaseVersion] = appendVoteRef{offset: offset, hash: frame.hash, digest: plan.digest, signature: plan.vote.Signature}
		s.lastVote, s.lastDigest = plan.vote, plan.digest
	}
	s.version, s.head, s.end = plan.version, frame.hash, frame.next
	s.index++
	return nil
}

func (s *AppendSigner) loadVote(slot uint64, ref appendVoteRef) (*voteRecord, error) {
	if s.lastVote != nil && s.lastVote.Batch.BaseVersion == slot && s.lastDigest == ref.digest {
		return s.lastVote, nil // Private, fully validated and immutable.
	}
	prefix := make([]byte, appendFrameSize)
	if _, err := s.file.ReadAt(prefix, ref.offset); err != nil {
		return nil, err
	}
	var previous [32]byte
	copy(previous[:], prefix[16:48])
	frame, err := readAppendFrame(s.file, ref.offset, s.end, previous)
	if err != nil || frame.hash != ref.hash {
		return nil, fmt.Errorf("durable vote frame changed/corrupt")
	}
	var event appendTransition
	if err := decodeAppend(frame.payload, &event); err != nil || event.Vote == nil {
		return nil, fmt.Errorf("durable vote missing/corrupt")
	}
	digest, err := s.checkVote(*event.Vote, slot)
	if err != nil || digest != ref.digest || !bytes.Equal(event.Vote.Signature.Value, ref.signature.Value) {
		return nil, fmt.Errorf("durable vote identity changed/corrupt")
	}
	return event.Vote, nil // Do not cache historical full witnesses.
}

func (s *AppendSigner) replay(repairShortTail bool, view *View) error {
	info, err := s.file.Stat()
	if err != nil {
		return err
	}
	size := info.Size()
	header, err := readAppendFrame(s.file, 0, size, [32]byte{})
	if err != nil {
		return fmt.Errorf("signer WAL header unavailable: %w", err)
	}
	var pinned appendHeader
	if err := decodeAppend(header.payload, &pinned); err != nil {
		return err
	}
	expected, _ := json.Marshal(s.header())
	actual, _ := json.Marshal(pinned)
	if !bytes.Equal(expected, actual) {
		return fmt.Errorf("signer WAL configuration/member/policy mismatch")
	}
	s.head, s.end = header.hash, header.next
	for s.end < size {
		offset := s.end
		frame, err := readAppendFrame(s.file, offset, size, s.head)
		if errors.Is(err, errAppendShortTail) && repairShortTail {
			return s.sealShortTail(offset, size)
		}
		if err != nil {
			return err // Complete corrupt frames are NEVER skipped or repaired.
		}
		var event appendTransition
		if err := decodeAppend(frame.payload, &event); err != nil {
			return err
		}
		if event.Schema != appendWALSchema || event.Index != s.index+1 || event.BaseVersion != s.version || len(event.Parents) == 0 && event.Vote == nil {
			return fmt.Errorf("WAL transition schema/index/prefix corrupt")
		}
		parents, err := verifyAppendParents(s.verifier, event.Parents)
		if err != nil {
			return err
		}
		var batch *Batch
		var signature *Signature
		var digest string
		if event.Vote != nil {
			digest, err = s.checkVote(*event.Vote, event.Vote.Batch.BaseVersion)
			if err != nil {
				return err
			}
			batch, signature = &event.Vote.Batch, &event.Vote.Signature
		}
		plan, err := s.prepare(parents, batch, digest, signature)
		if err != nil || len(plan.parents) != len(event.Parents) || (plan.vote != nil) != (event.Vote != nil) {
			return fmt.Errorf("WAL transition contract corrupt: %v", err)
		}
		if err := s.publish(plan, offset, frame, view); err != nil {
			return err
		}
	}
	return nil
}

// Only a final physically incomplete frame is recoverable. Its exact bytes are
// sealed durably BEFORE truncation. A complete checksum failure is fail-closed.
// This is crash-tail recovery, not tolerance of external history rollback.
func (s *AppendSigner) sealShortTail(offset, size int64) error {
	archive, err := os.CreateTemp(filepath.Dir(s.path), filepath.Base(s.path)+".torn-*")
	if err != nil {
		return err
	}
	defer archive.Close()
	if _, err := io.Copy(archive, io.NewSectionReader(s.file, offset, size-offset)); err != nil {
		return err
	}
	if err := archive.Sync(); err != nil {
		return err
	}
	if err := syncAppendDirectory(archive.Name()); err != nil {
		return err
	}
	if err := s.file.Truncate(offset); err != nil {
		return err
	}
	return s.file.Sync()
}
