package ownership

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

// MemberRequest is a bounded local-IPC message, not an authenticated network
// protocol. Access to the member's stdin is a trusted OS/operator boundary.
type MemberRequest struct {
	Action       string        `json:"action"`
	Batch        *Batch        `json:"batch,omitempty"`
	Certificate  *Certificate  `json:"certificate,omitempty"`
	Certificates []Certificate `json:"certificates,omitempty"`
}

type MemberResponse struct {
	Status          string     `json:"status"`
	Error           string     `json:"error,omitempty"`
	Signature       *Signature `json:"signature,omitempty"`
	Changed         bool       `json:"changed,omitempty"`
	Version         uint64     `json:"version"`
	PlacementPolicy string     `json:"placement_policy,omitempty"`
	StorageMode     string     `json:"storage_mode,omitempty"`
}

// ServeMember exposes the signer over line-delimited stdin/stdout. It cannot
// hold other members' keys, fabricate their signatures or unlock split votes.
// Errors reject ONE request; corrupt/uncertain storage remains fenced by Signer.
func ServeMember(s MemberSigner, input io.Reader, output io.Writer) error {
	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 4096), 8<<20)
	encoder := json.NewEncoder(output)
	for scanner.Scan() {
		response := memberResponse(s, scanner.Bytes())
		if err := encoder.Encode(response); err != nil {
			return err
		}
	}
	return scanner.Err()
}

func memberResponse(s MemberSigner, raw []byte) MemberResponse {
	var request MemberRequest
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	err := decoder.Decode(&request)
	if err == nil && decoder.Decode(new(interface{})) != io.EOF {
		err = fmt.Errorf("trailing request data")
	}
	response := MemberResponse{Status: "FAIL", StorageMode: s.storageMode()}
	if s.placementRequired() {
		response.PlacementPolicy = PlacementWitnessSchema
	}
	if err == nil {
		switch request.Action {
		case "vote":
			if request.Batch == nil || request.Certificate != nil || len(request.Certificates) != 0 {
				err = fmt.Errorf("vote requires only a batch")
				break
			}
			var signature Signature
			signature, err = s.Vote(*request.Batch)
			if err == nil {
				response.Signature = &signature
			}
		case "advance_vote":
			if request.Batch == nil || request.Certificate != nil && len(request.Certificates) != 0 {
				err = fmt.Errorf("advance_vote requires a batch and one parent representation")
				break
			}
			parents := request.Certificates
			if request.Certificate != nil {
				parents = []Certificate{*request.Certificate}
			}
			var signature Signature
			signature, err = s.AdvanceVote(parents, *request.Batch)
			if err == nil {
				response.Signature = &signature
			}
		case "commit":
			if request.Certificate == nil || request.Batch != nil || len(request.Certificates) != 0 {
				err = fmt.Errorf("commit requires only a certificate")
				break
			}
			response.Changed, err = s.Commit(*request.Certificate)
		case "status":
			if request.Batch != nil || request.Certificate != nil || len(request.Certificates) != 0 {
				err = fmt.Errorf("status takes no payload")
			}
		default:
			err = fmt.Errorf("unknown member action")
		}
	}
	if err == nil {
		response.Version, err = s.Version()
	}
	if err != nil {
		response.Error = err.Error()
		response.Signature = nil
	} else {
		response.Status = "PASS"
	}
	return response
}
